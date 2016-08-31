/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.converter

import de.qaware.chronix.converter.common.Compression
import de.qaware.chronix.converter.common.DoubleList
import de.qaware.chronix.converter.common.LongList
import de.qaware.chronix.converter.serializer.ProtoBufKassiopeiaSimpleSerializer
import de.qaware.chronix.converter.serializer.gen.SimpleProtocolBuffers
import de.qaware.chronix.timeseries.MetricTimeSeries
import de.qaware.chronix.timeseries.Point
import spock.lang.Specification

import java.text.DecimalFormat
import java.time.Instant
import java.util.zip.GZIPInputStream

/**
 * Benchmark to evaluate the error of different almost_equals aberrations
 * @author f.lautenschlager
 */
class KassiopeiaErrorRateBenchmark extends Specification {

    //TODO: Move this to the tuning repo

    def "execute error rate benchmark"() {

        given:
        def timeSeriesList = readTimeSeriesData()

        when:

        timeSeriesList.each {

            def rawTimeSeries = it.value
            rawTimeSeries.sort()

            [265, 512, 1024, 2048, 4096].eachWithIndex { def chunkSize, def chunkIndex ->

                println "============================================================="
                println "===================    ${chunkSize} KB      ========================="
                println "============================================================="


                [10, 25, 50, 100].eachWithIndex { def almostEquals, def almostEqualsIndex ->

                    println "============================================================="
                    println "===================     ${almostEquals} ms       ========================="
                    println "============================================================="

                    def modifiedTimeSeries = serializeAndDeserialize(rawTimeSeries, chunkSize, almostEquals)

                    def modTimeSeries = new MetricTimeSeries.Builder("${rawTimeSeries.getMetric()}-concated").build()

                    modifiedTimeSeries.each { ts ->
                        LongList timeStamps = ts.getTimestamps()
                        DoubleList values = ts.getValues()
                        modTimeSeries.addAll(timeStamps, values)
                    }

                    modTimeSeries.sort()
                    modTimeSeries.getEnd()

                    println "Raw time series ${rawTimeSeries.getMetric()} with almost_equals of $almostEquals milliseconds"
                    println "Mod time series ${modTimeSeries.getMetric()} with almost_equals of $almostEquals milliseconds"

                    long sumTimeRaw = 0;
                    long sumTimeMod = 0;

                    rawTimeSeries.points().each {
                        sumTimeRaw += it.timestamp
                    }

                    modTimeSeries.points().each {
                        sumTimeMod += it.timestamp
                    }

                    def maxDeviation = 0;
                    def indexwiseDeltaRaw = 0;
                    def indexwiseDeltaMod = 0;
                    def size = rawTimeSeries.size()
                    def modSize = modTimeSeries.size()

                    if (size != modSize) {
                        println "Sizes are not equals. Raw ${size}, modified ${modSize}"
                    }


                    for (int j = 0; j < rawTimeSeries.size(); j++) {

                        def rawTS = rawTimeSeries.getTime(j);
                        def modTS = modTimeSeries.getTime(j);

                        def deviation = Math.abs(rawTS - modTS);

                        if (deviation > maxDeviation) {
                            maxDeviation = deviation;
                        }

                        if (j + 1 < size) {
                            indexwiseDeltaRaw += Math.abs(rawTimeSeries.getTime(j + 1) - rawTS)
                            indexwiseDeltaMod += Math.abs(modTimeSeries.getTime(j + 1) - modTS)
                        }

                    }
                    //to mins
                    sumTimeRaw = sumTimeRaw / 60000
                    sumTimeMod = sumTimeMod / 60000
                    indexwiseDeltaRaw = indexwiseDeltaRaw / 60000
                    indexwiseDeltaMod = indexwiseDeltaMod / 60000

                    def delta = Math.abs(sumTimeMod - sumTimeRaw)
                    def deltaIndexwiseDeltas = indexwiseDeltaMod - indexwiseDeltaRaw;

                    println "Raw: ${rawTimeSeries.getMetric()} has $size points starting at ${Instant.ofEpochMilli(rawTimeSeries.start)} and ending at ${Instant.ofEpochMilli(rawTimeSeries.end)}"
                    println "Mod: ${modTimeSeries.getMetric()} has $size points starting at ${Instant.ofEpochMilli(modTimeSeries.start)} and ending at ${Instant.ofEpochMilli(modTimeSeries.end)}"

                    println "Raw: $sumTimeRaw (Sum raw time) in minutes"
                    println "Mod: $sumTimeMod (Sum mod time) in minutes"
                    println "Delta (total): $delta"
                    println "Percent (delta / sumTimeRaw): ${delta / sumTimeRaw}"
                    println "Max deviation: $maxDeviation in milliseconds"
                    println "Raw: Sum of deltas: $indexwiseDeltaRaw in minutes"
                    println "Mod: Sum of deltas: $indexwiseDeltaMod in minutes"
                    println "Difference of the deltas: $deltaIndexwiseDeltas in minutes"
                    println "Percent ${deltaIndexwiseDeltas / indexwiseDeltaRaw}"
                }

            }
        }

        then:
        1 == 1

    }

    static
    def List<MetricTimeSeries> serializeAndDeserialize(MetricTimeSeries rawTimeSeries, long chunkSize, long almostEquals) {

        long sizeOfAPoint = SimpleProtocolBuffers.Point.newBuilder().setT(Instant.now().toEpochMilli()).setV(23746282d).build().serializedSize;
        long sizeOfaEmptyList = SimpleProtocolBuffers.Points.newBuilder().build().serializedSize;

        long amountOfPoints = (chunkSize - sizeOfaEmptyList) / sizeOfAPoint;

        List<List<Point>> chunks = new ArrayList<>()

        List<Point> workingList = new ArrayList<>((int) amountOfPoints)

        def calcAmountOfPoints = amountOfPoints;

        rawTimeSeries.points().eachWithIndex { Point entry, int index ->

            workingList.add(entry)

            if (workingList.size() == amountOfPoints) {
                chunks.add(new ArrayList<Point>(workingList))
                workingList.clear()
                calcAmountOfPoints = amountOfPoints
            }
        }
        chunks.add(new ArrayList<Point>(workingList))

        List<MetricTimeSeries> modChunks = new ArrayList<>()

        def size = 0;
        for (int i = 0; i < chunks.size(); i++) {
            def chunk = chunks.get(i)

            def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(chunk.iterator(), almostEquals)
            size += compressedProtoPoints.length

            def builder = new MetricTimeSeries.Builder(rawTimeSeries.metric);
            def start = chunk.get(0).timestamp;
            def end = chunk.get(chunk.size() - 1).timestamp;

            ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, start, end, almostEquals, builder)

            def recovertedChunk = builder.build()
            if (recovertedChunk.size() != chunk.size()) {
                println "Processing chunk at index $i"
                println "Recovered chunk and original chunk have not the same size. Recoverd ${recovertedChunk.size()}, Original ${chunk.size()}"
            }

            modChunks.add(builder.build())
        }

        def points = asProtoBuf(rawTimeSeries)
        def compressed = Compression.compress(points.toByteArray())
        println "Raw serialized size ${points.serializedSize} bytes"
        println "Raw serialized compressed size ${compressed.length} bytes"
        println "Modified time series size ${size} bytes"
        println "Delta to compressed is ${compressed.length - size} bytes"

        return modChunks
    }

    static def asProtoBuf(MetricTimeSeries metricTimeSeries) {
        def builder = SimpleProtocolBuffers.Points.newBuilder();
        def point = SimpleProtocolBuffers.Point.newBuilder();

        metricTimeSeries.points().each { p ->
            builder.addP(point.setT(p.timestamp).setV(p.value).build())
        }

        builder.build();

    }

    static def readTimeSeriesData() {
        def url = KassiopeiaErrorRateBenchmark.getResource("/data-mini");
        def tsDir = new File(url.toURI())

        def documents = new HashMap<String, MetricTimeSeries>()

        tsDir.listFiles().each { File file ->
            println("Processing file $file")
            documents.put(file.name, new MetricTimeSeries.Builder(file.name).build())

            def nf = DecimalFormat.getInstance(Locale.ENGLISH);
            def filePoints = 0

            def unzipped = new GZIPInputStream(new FileInputStream(file));

            unzipped.splitEachLine(";") { fields ->
                //Its the first line of a csv file
                if ("Date" != fields[0]) {
                    //First field is the timestamp: 26.08.2013 00:00:17.361
                    def date = Instant.parse(fields[0])
                    fields.subList(1, fields.size()).eachWithIndex { String value, int i ->
                        documents.get(file.name).add(date.toEpochMilli(), nf.parse(value).doubleValue())
                        filePoints = i
                    }
                }
            }

        }
        documents
    }

}
