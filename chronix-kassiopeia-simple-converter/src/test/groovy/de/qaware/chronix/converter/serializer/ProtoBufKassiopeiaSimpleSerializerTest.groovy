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
package de.qaware.chronix.converter.serializer

import de.qaware.chronix.timeseries.MetricTimeSeries
import de.qaware.chronix.timeseries.Point
import spock.lang.Shared
import spock.lang.Specification

import java.text.DecimalFormat
import java.time.Instant
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream

/**
 * Unit test for the protocol buffers serializer
 * @author f.lautenschlager
 */
class ProtoBufKassiopeiaSimpleSerializerTest extends Specification {

    def "test from without range query"() {
        given:
        def points = []
        100.times {
            points.add(new Point(it, it + 1, it * 100))
        }
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())

        when:
        def builder = new MetricTimeSeries.Builder("metric");
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, 0, points.size(), 0, points.size(), builder)
        def ts = builder.build();
        then:
        100.times {
            ts.getValue(it) == it * 100
            ts.getTime(it) == it
        }
    }

    @Shared
    def start = Instant.now()
    @Shared
    def end = start.plusSeconds(100 * 100)

    def "test from with range query"() {
        given:
        def points = []

        100.times {
            points.add(new Point(it, start.plusSeconds(it).toEpochMilli(), it * 100))
        }
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())
        def builder = new MetricTimeSeries.Builder("metric");

        when:
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, start.toEpochMilli(), end.toEpochMilli(), from, to, builder)
        def ts = builder.build();

        then:
        List<Point> list = ts.points().collect(Collectors.toList())
        list.size() == size
        if (size == 21) {
            list.get(0).timestamp == 1456394850774
            list.get(0).value == 5000.0d

            list.get(20).timestamp == 1456394870774
            list.get(20).value == 7000.0d
        }

        where:
        from << [end.toEpochMilli() + 2, 0, start.toEpochMilli() + 4, start.plusSeconds(50).toEpochMilli()]
        to << [end.toEpochMilli() + 3, 0, start.toEpochMilli() + 2, start.plusSeconds(70).toEpochMilli()]
        size << [0, 0, 0, 21]
    }

    def "test convert to protocol buffers points"() {
        given:
        def points = []
        100.times {
            points.add(new Point(it, it + 15, it * 100))
        }
        //Points that are null are ignored
        points.add(null)
        def builder = new MetricTimeSeries.Builder("");
        when:
        def compressedPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())
        ProtoBufKassiopeiaSimpleSerializer.from(compressedPoints, 0, 114, builder)

        then:
        builder.build().size() == 100
    }

    def "test iterator with invalid arguments"() {
        when:
        ProtoBufKassiopeiaSimpleSerializer.from(null, 0, 0, from, to, new MetricTimeSeries.Builder(""))
        then:
        thrown IllegalArgumentException
        where:
        from << [-1, 0, -1]
        to << [0, -1, -1]

    }


    def "test private constructor"() {
        when:
        ProtoBufKassiopeiaSimpleSerializer.newInstance()
        then:
        noExceptionThrown()
    }

    def "test date-delta-compaction with almost_equals = 0"() {
        given:
        def points = []
        points.add(new Point(0, 1, 10))
        points.add(new Point(1, 5, 20))
        points.add(new Point(2, 8, 30))
        points.add(new Point(3, 16, 40))
        points.add(new Point(4, 21, 50))

        def builder = new MetricTimeSeries.Builder("metric");

        when:
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator(), 0l)
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, 1l, 1036l, 1l, 1036l, 0l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:
        listPoints.get(0).timestamp == 1
        listPoints.get(1).timestamp == 5
        listPoints.get(2).timestamp == 8
        listPoints.get(3).timestamp == 16
        listPoints.get(4).timestamp == 21

    }

    def "test date-delta-compaction used in the paper"() {
        given:
        def points = []
        points.add(new Point(0, 1, 10))
        points.add(new Point(1, 5, 20))
        points.add(new Point(2, 8, 30))
        points.add(new Point(3, 16, 40))
        points.add(new Point(4, 21, 50))

        def builder = new MetricTimeSeries.Builder("metric");

        when:
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator(), 4l)
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, 1l, 1036l, 1l, 1036l, 4l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:
        listPoints.get(0).timestamp == 1//offset: 4
        listPoints.get(1).timestamp == 5//offset: 4
        listPoints.get(2).timestamp == 9//offset: 4
        listPoints.get(3).timestamp == 16//offset: 7
        listPoints.get(4).timestamp == 23//offset: 7

    }


    def "test date-delta-compaction"() {
        given:
        def points = []
        points.add(new Point(0, 10, -10))
        points.add(new Point(1, 20, -20))
        points.add(new Point(2, 30, -30))
        points.add(new Point(3, 39, -39))
        points.add(new Point(4, 48, -48))
        points.add(new Point(5, 57, -57))
        points.add(new Point(6, 66, -66))
        points.add(new Point(7, 75, -75))
        points.add(new Point(8, 84, -84))
        points.add(new Point(9, 93, -93))
        points.add(new Point(10, 102, -102))
        points.add(new Point(11, 111, -109))
        points.add(new Point(12, 120, -118))
        points.add(new Point(13, 129, -127))
        points.add(new Point(14, 138, -136))

        def builder = new MetricTimeSeries.Builder("metric");

        when:
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator(), 10l)
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, 10l, 1036l, 10l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:                            //diff to origin
        listPoints.get(0).timestamp == 10//0
        listPoints.get(1).timestamp == 20//0
        listPoints.get(2).timestamp == 30//0
        listPoints.get(3).timestamp == 40//1
        listPoints.get(4).timestamp == 50//2
        listPoints.get(5).timestamp == 60//3
        listPoints.get(6).timestamp == 70//4
        listPoints.get(7).timestamp == 75//5 drift detected
        listPoints.get(8).timestamp == 85//5 drift
        listPoints.get(9).timestamp == 95//5 drift
        listPoints.get(10).timestamp == 105//5
        listPoints.get(11).timestamp == 115//5
        listPoints.get(12).timestamp == 120//5
        listPoints.get(13).timestamp == 130//5
        listPoints.get(14).timestamp == 140//5
    }


    def "test date-delta-compaction with different values"() {
        given:
        def points = []
        points.add(new Point(0, 1462892410, -10))
        points.add(new Point(1, 1462892420, -20))
        points.add(new Point(2, 1462892430, -30))
        points.add(new Point(3, 1462892439, -39))
        points.add(new Point(4, 1462892448, -48))
        points.add(new Point(5, 1462892457, -57))
        points.add(new Point(6, 1462892466, -66))
        points.add(new Point(7, 1462892475, -75))
        points.add(new Point(8, 1462892484, -84))
        points.add(new Point(9, 1462892493, -93))
        points.add(new Point(10, 1462892502, -102))
        points.add(new Point(11, 1462892511, -109))
        points.add(new Point(12, 1462892520, -118))
        points.add(new Point(13, 1462892529, -127))
        points.add(new Point(14, 1462892538, -136))

        def builder = new MetricTimeSeries.Builder("metric1");

        when:
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, 1462892410l, 1462892543l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:                            //diff to origin
        listPoints.get(0).timestamp == 1462892410//0
        listPoints.get(1).timestamp == 1462892420//0
        listPoints.get(2).timestamp == 1462892430//0
        listPoints.get(3).timestamp == 1462892440//1
        listPoints.get(4).timestamp == 1462892450//2
        listPoints.get(5).timestamp == 1462892460//3
        listPoints.get(6).timestamp == 1462892470//4
        listPoints.get(7).timestamp == 1462892475//5
        listPoints.get(8).timestamp == 1462892485//5
        listPoints.get(9).timestamp == 1462892495//5
        listPoints.get(10).timestamp == 1462892505//5
        listPoints.get(11).timestamp == 1462892515//5
        listPoints.get(12).timestamp == 1462892520//5
        listPoints.get(13).timestamp == 1462892530//5
        listPoints.get(14).timestamp == 1462892540//5
    }


    def "test raw time series with almost_equals = 0"() {
        given:
        def rawTimeSeriesList = readTimeSeriesData()

        when:

        rawTimeSeriesList.each {
            def rawTimeSeries = it.value;
            rawTimeSeries.sort()

            def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(rawTimeSeries.points().iterator(), 0l)
            def builder = new MetricTimeSeries.Builder("heap");

            ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, rawTimeSeries.start, rawTimeSeries.end, 0l, builder)
            def modifiedTimeSeries = builder.build()

            def count = rawTimeSeries.size();
            println "Checking $count points for almost_equals = 0"

            for (int i = 0; i < count; i++) {
                long delta = rawTimeSeries.getTime(i) - modifiedTimeSeries.getTime(i)
                if (rawTimeSeries.getTime(i) != modifiedTimeSeries.getTime(i)) {
                    throw new IllegalStateException("Points are not equals at " + i + ". Should " + rawTimeSeries.getTime(i) + " but is " + modifiedTimeSeries.getTime(i) + " a delta of " + delta);
                }
            }
        }
        then:
        noExceptionThrown()

    }


    static def readTimeSeriesData() {
        def url = ProtoBufKassiopeiaSimpleSerializerTest.getResource("/data-mini");
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
