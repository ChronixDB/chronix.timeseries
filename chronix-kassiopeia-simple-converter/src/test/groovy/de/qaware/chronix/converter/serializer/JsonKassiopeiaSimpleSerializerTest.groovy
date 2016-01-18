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

import de.qaware.chronix.timeseries.dt.DoubleList
import de.qaware.chronix.timeseries.dt.LongList
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.Charset

/**
 * Unit test for the kassiopeia serializer
 * @author f.lautenschlager
 */
class JsonKassiopeiaSimpleSerializerTest extends Specification {

    def "test serialize to and deserialize from json"() {
        given:
        def times = [0l, 1l, 2l]
        def values = [4711, 8564, 1237]
        def serializer = new JsonKassiopeiaSimpleSerializer()
        def start = 0l
        def end = 2l

        when:
        def json = serializer.toJson(times.stream(), values.stream())

        then:
        def deserialize = serializer.fromJson(json, start, end)

        def serTimes = deserialize[0] as LongList
        def serValues = deserialize[1] as DoubleList

        times.size() == 3
        serValues.size() == 3

        serTimes.get(0) == 0l
        serTimes.get(1) == 1l
        serTimes.get(2) == 2l

        serValues.get(0) == 4711d
        serValues.get(1) == 8564d
        serValues.get(2) == 1237d
    }

    @Unroll
    def "test serialize and deserialize from json with filter #start and #end expecting #size elements"() {
        given:
        def times = [0l, 1l, 2l, 3l, 4l, 5l]
        def values = [4711, 8564, 1237, 1237, 1237, 1237]
        def serializer = new JsonKassiopeiaSimpleSerializer()

        when:
        def json = serializer.toJson(times.stream(), values.stream())
        def deserialize = serializer.fromJson(json, start, end)
        then:
        (deserialize[0] as LongList).size() == size

        where:
        start << [0, 1, 1, 0]
        end << [0, 1, 3, 6]
        size << [0, 1, 3, 6]
    }

    def "test serialize to json with empty timestamps / values"() {
        given:
        def serializer = new JsonKassiopeiaSimpleSerializer()

        when:
        def json = serializer.toJson(times, values)

        then:
        new String(json) == "[[],[]]"

        where:
        times << [null, [0l, 1l, 2l].stream()]
        values << [[0l, 1l, 2l].stream(), null]
    }

    def "test deserialize from empty json "() {
        given:
        def serializer = new JsonKassiopeiaSimpleSerializer()

        when:
        def result = serializer.fromJson("[[],[]]".getBytes(Charset.forName("UTF-8")), 1, 2000)

        then:
        result.size() == 2
        (result[0] as LongList).size() == 0
        (result[1] as DoubleList).size() == 0
    }
}
