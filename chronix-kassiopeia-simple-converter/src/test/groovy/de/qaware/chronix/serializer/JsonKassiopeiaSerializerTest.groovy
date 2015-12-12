/*
 * Copyright (C) 2015 QAware GmbH
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
package de.qaware.chronix.serializer

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for the kassiopeia serializer
 * @author f.lautenschlager
 */
class JsonKassiopeiaSerializerTest extends Specification {

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

        def serTimes = deserialize[0]
        def serValues = deserialize[1]

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
        deserialize[0].size() == size

        where:
        start << [0, 1, 1, 0]
        end << [0, 1, 3, 6]
        size << [0, 1, 3, 6]


    }
}
