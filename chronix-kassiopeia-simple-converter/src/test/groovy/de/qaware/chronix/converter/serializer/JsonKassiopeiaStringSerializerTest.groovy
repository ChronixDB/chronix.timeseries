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

import de.qaware.chronix.converter.common.LongList
import de.qaware.chronix.timeseries.StringTimeSeries
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.Charset

/**
 * Unit test for the kassiopeia serializer
 * @author f.lautenschlager & m.jalowski
 */
class JsonKassiopeiaStringSerializerTest extends Specification {

    def "test serialize to and deserialize from json"() {
        given:
        def times = longList([0, 1, 2])
        def values = ["string0", "string1", "string2"]
        def ts = new StringTimeSeries.Builder("test").points(times, values).build()

        def serializer = new JsonKassiopeiaStringSerializer()
        def start = 0l
        def end = 2l

        when:
        def json = serializer.toJson(ts)

        then:
        def builder = new StringTimeSeries.Builder("test")
        serializer.fromJson(json, start, end, builder)
        def recoverdTs = builder.build();


        times.size() == 3
        recoverdTs.size() == 3

        recoverdTs.getTime(0) == 0l
        recoverdTs.getTime(1) == 1l
        recoverdTs.getTime(2) == 2l

        recoverdTs.getValue(0) == "string0"
        recoverdTs.getValue(1) == "string1"
        recoverdTs.getValue(2) == "string2"
    }

    @Unroll
    def "test serialize and deserialize from json with filter #start and #end expecting #size elements"() {
        given:
        def times = longList([0, 1, 2, 3, 4, 5])
        def values = ["string0", "string1", "string2", "string3", "string4", "string5"]
        def ts = new StringTimeSeries.Builder("test").points(times, values).build()

        def serializer = new JsonKassiopeiaStringSerializer()

        when:
        def builder = new StringTimeSeries.Builder("test")

        def json = serializer.toJson(ts)
        serializer.fromJson(json, start, end, builder)
        then:
        builder.build().size() == size

        where:
        start << [0, 1, 1, 0]
        end << [0, 1, 3, 6]
        size << [0, 1, 3, 6]
    }

    def "test serialize to json with empty timestamps / values"() {
        given:
        def serializer = new JsonKassiopeiaStringSerializer()
        def ts = new StringTimeSeries.Builder("test").points(times, values).build()

        when:
        def json = serializer.toJson(ts)

        then:
        new String(json) == "[[],[]]"

        where:
        times << [null, longList([0l, 1l, 2l]) as LongList]
        values << [["string0", "string1", "string2"], null]
    }

    def longList(ArrayList<Long> longs) {
        def times = new LongList()
        longs.each { l -> times.add(l) }

        times
    }

    def "test deserialize from empty json "() {
        given:
        def serializer = new JsonKassiopeiaStringSerializer()
        def builder = new StringTimeSeries.Builder("test")

        when:
        def result = serializer.fromJson("[[],[]]".getBytes(Charset.forName("UTF-8")), 1, 2000, builder)
        def ts = builder.build()
        then:
        ts.size() == 0
    }
}
