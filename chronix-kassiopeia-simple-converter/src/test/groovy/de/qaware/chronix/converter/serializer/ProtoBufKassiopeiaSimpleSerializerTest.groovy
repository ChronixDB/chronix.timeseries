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

import de.qaware.chronix.timeseries.dt.Pair
import spock.lang.Specification

import java.time.Instant

/**
 * Unit test for the protocol buffers serializer
 * @author f.lautenschlager
 */
class ProtoBufKassiopeiaSimpleSerializerTest extends Specification {
    def "test from without range query"() {
        given:
        def points = []
        100.times {
            points.add(new Pair(it, it + 1, it * 100))
        }
        def protoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())

        when:
        def pairs = ProtoBufKassiopeiaSimpleSerializer.from(protoPoints.toByteString().newInput(), 0, points.size())
        then:
        100.times {
            def point = pairs.next()
            point.index = it
            point.timestamp = it
            point.value = it * 100
        }
    }

    def "test from with range query"() {
        given:
        def points = []
        def start = Instant.now()
        def end = start.plusSeconds(100 * 100)
        100.times {
            points.add(new Pair(it, start.plusSeconds(it).toEpochMilli(), it * 100))
        }
        def protoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())

        def queryStart = start.plusSeconds(50).toEpochMilli()
        def queryEnd = start.plusSeconds(70).toEpochMilli()

        when:
        def pairs = ProtoBufKassiopeiaSimpleSerializer.from(protoPoints.toByteString().newInput(), start.toEpochMilli(), end.toEpochMilli(), queryStart, queryEnd)
        then:
        //should do nothing
        pairs.remove()
        def list = pairs.toList()
        list.size() == 21
        list.get(0).index == 50
        list.get(20).index == 70
    }

    def "test convert to protocol buffers points"() {
        given:
        def points = []
        100.times {
            points.add(new Pair(it, it, it * 100))
        }
        //Points that are null are ignored
        points.add(null)

        when:
        def protoPoints = ProtoBufKassiopeiaSimpleSerializer.to(points.iterator())

        then:
        protoPoints.getPList().size() == 100
    }

    def "test iterator with invalid arguments"() {
        when:
        ProtoBufKassiopeiaSimpleSerializer.from(null, 0, 0, from, to)
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

    def "test exception case for point iterator (inner class)"() {
        when:
        ProtoBufKassiopeiaSimpleSerializer.PointIterator.newInstance(new ByteArrayInputStream("some".bytes), 1, 2, 3, 4)
        then:
        thrown IllegalStateException
    }
}
