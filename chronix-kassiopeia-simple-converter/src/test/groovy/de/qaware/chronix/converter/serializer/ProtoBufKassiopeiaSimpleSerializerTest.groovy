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

import de.qaware.chronix.converter.common.Compression
import de.qaware.chronix.timeseries.MetricTimeSeries
import de.qaware.chronix.timeseries.dt.Point
import spock.lang.Shared
import spock.lang.Specification

import java.time.Instant
import java.util.stream.Collectors

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
            ts.get(it) == it * 100
            ts.getTimestamps().get(it) == it
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
        def compressedProtoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())
        ProtoBufKassiopeiaSimpleSerializer.from(compressedProtoPoints, 10l, 1036l, builder)
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
        listPoints.get(7).timestamp == 80//5
        listPoints.get(8).timestamp == 89//5
        listPoints.get(9).timestamp == 98//5
        listPoints.get(10).timestamp == 107//5
        listPoints.get(11).timestamp == 116//5
        listPoints.get(12).timestamp == 125//5
        listPoints.get(13).timestamp == 134//5
        listPoints.get(14).timestamp == 143//5
    }

}
