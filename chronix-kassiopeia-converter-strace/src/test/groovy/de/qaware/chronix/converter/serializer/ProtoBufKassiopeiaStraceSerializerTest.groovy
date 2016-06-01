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

import de.qaware.chronix.timeseries.StraceTimeSeries
import de.qaware.chronix.timeseries.dt.StracePoint
import spock.lang.Shared
import spock.lang.Specification

import java.time.Instant
import java.util.stream.Collectors

/**
 * Unit test for the protocol buffers serializer
 * @author f.lautenschlager & m.jalowski
 */
class ProtoBufKassiopeiaStraceSerializerTest  extends Specification {

    def "test from without range query"() {
        given:
        def points = []
        100.times {
            points.add(new StracePoint(it, it + 1, it * 100 + " as string"))
        }
        def compressedProtoPoints = ProtoBufKassiopeiaStraceSerializer.to(points.iterator())

        when:
        def builder = new StraceTimeSeries.Builder("metric");
        ProtoBufKassiopeiaStraceSerializer.from(compressedProtoPoints, 0, points.size(), 0, points.size(), builder)
        def ts = builder.build();
        then:
        100.times {
            ts.getValue(it) == it * 100 + " as string"
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
            points.add(new StracePoint(it, start.plusSeconds(it).toEpochMilli(), it * 100 + " as string"))
        }
        def compressedProtoPoints = ProtoBufKassiopeiaStraceSerializer.to(points.iterator())
        def builder = new StraceTimeSeries.Builder("metric");

        when:
        ProtoBufKassiopeiaStraceSerializer.from(compressedProtoPoints, start.toEpochMilli(), end.toEpochMilli(), from, to, builder)
        def ts = builder.build();

        then:
        List<StracePoint> list = ts.points().collect(Collectors.toList())
        list.size() == size
        if (size == 21) {
            list.get(0).timestamp == 1456394850774
            list.get(0).value == 5000 + " as string"

            list.get(20).timestamp == 1456394870774
            list.get(20).value == 7000 + " as string"
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
            points.add(new StracePoint(it, it + 15, it * 100 + " as string"))
        }
        //Points that are null are ignored
        points.add(null)
        def builder = new StraceTimeSeries.Builder("");
        when:
        def compressedPoints = ProtoBufKassiopeiaStraceSerializer.to(points.iterator())
        ProtoBufKassiopeiaStraceSerializer.from(compressedPoints, 0, 114, builder)

        then:
        builder.build().size() == 100
    }

    def "test iterator with invalid arguments"() {
        when:
        ProtoBufKassiopeiaStraceSerializer.from(null, 0, 0, from, to, new StraceTimeSeries.Builder(""))
        then:
        thrown IllegalArgumentException
        where:
        from << [-1, 0, -1]
        to << [0, -1, -1]

    }


    def "test private constructor"() {
        when:
        ProtoBufKassiopeiaStraceSerializer.newInstance()
        then:
        noExceptionThrown()
    }


    def "test date-delta-compaction"() {
        given:
        def points = []
        points.add(new StracePoint(0, 10, "string0"))
        points.add(new StracePoint(1, 20, "string1"))
        points.add(new StracePoint(2, 30, "string2"))
        points.add(new StracePoint(3, 39, "string3"))
        points.add(new StracePoint(4, 48, "string4"))
        points.add(new StracePoint(5, 57, "string5"))
        points.add(new StracePoint(6, 66, "string6"))
        points.add(new StracePoint(7, 75, "string7"))
        points.add(new StracePoint(8, 84, "string8"))
        points.add(new StracePoint(9, 93, "string9"))
        points.add(new StracePoint(10, 102, "string10"))
        points.add(new StracePoint(11, 111, "string11"))
        points.add(new StracePoint(12, 120, "string12"))
        points.add(new StracePoint(13, 129, "string13"))
        points.add(new StracePoint(14, 138, "string14"))

        def builder = new StraceTimeSeries.Builder("metric");

        when:
        def compressedProtoPoints = ProtoBufKassiopeiaStraceSerializer.to(points.iterator())
        ProtoBufKassiopeiaStraceSerializer.from(compressedProtoPoints, 10l, 1036l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<StracePoint>

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


    def "test date-delta-compaction with different values"() {
        given:
        def points = []
        points.add(new StracePoint(0, 1462892410, "string0"))
        points.add(new StracePoint(1, 1462892420, "string1"))
        points.add(new StracePoint(2, 1462892430, "string2"))
        points.add(new StracePoint(3, 1462892439, "string3"))
        points.add(new StracePoint(4, 1462892448, "string4"))
        points.add(new StracePoint(5, 1462892457, "string5"))
        points.add(new StracePoint(6, 1462892466, "string6"))
        points.add(new StracePoint(7, 1462892475, "string7"))
        points.add(new StracePoint(8, 1462892484, "string8"))
        points.add(new StracePoint(9, 1462892493, "string9"))
        points.add(new StracePoint(10, 1462892502, "string10"))
        points.add(new StracePoint(11, 1462892511, "string11"))
        points.add(new StracePoint(12, 1462892520, "string12"))
        points.add(new StracePoint(13, 1462892529, "string13"))
        points.add(new StracePoint(14, 1462892538, "string14"))

        def builder = new StraceTimeSeries.Builder("metric1");

        when:
        def compressedProtoPoints = ProtoBufKassiopeiaStraceSerializer.to(points.iterator())
        ProtoBufKassiopeiaStraceSerializer.from(compressedProtoPoints, 1462892410l, 1462892543l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<StracePoint>

        then:                            //diff to origin
        listPoints.get(0).timestamp == 1462892410//0
        listPoints.get(1).timestamp == 1462892420//0
        listPoints.get(2).timestamp == 1462892430//0
        listPoints.get(3).timestamp == 1462892440//1
        listPoints.get(4).timestamp == 1462892450//2
        listPoints.get(5).timestamp == 1462892460//3
        listPoints.get(6).timestamp == 1462892470//4
        listPoints.get(7).timestamp == 1462892480//5
        listPoints.get(8).timestamp == 1462892489//5
        listPoints.get(9).timestamp == 1462892498//5
        listPoints.get(10).timestamp == 1462892507//5
        listPoints.get(11).timestamp == 1462892516//5
        listPoints.get(12).timestamp == 1462892525//5
        listPoints.get(13).timestamp == 1462892534//5
        listPoints.get(14).timestamp == 1462892543//5
    }
}
