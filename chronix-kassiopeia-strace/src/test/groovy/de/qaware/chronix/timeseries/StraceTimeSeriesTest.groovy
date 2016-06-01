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
package de.qaware.chronix.timeseries

import de.qaware.chronix.timeseries.dt.LongList
import spock.lang.Specification
import spock.lang.Unroll

/**
 *
 */
class StraceTimeSeriesTest extends Specification {

    @Unroll
    def "test create a strace time series and access its values"() {

        given:
        def times = new LongList()
        def values = new ArrayList<String>()
        10.times {
            times.add(it as long)
            values.add("line " + it * 10 as String)
        }
        def attributes = new HashMap<String, Object>()
        attributes.put("cmd", "strace ls")

        when:
        def ts = new StraceTimeSeries.Builder("ls")
                .attributes(attributes)
                .attribute("prog", "ls")
                .points(times, values)
                .point(10 as long, "")
                .build()

        then:
        ts.start == 0
        ts.end == 10
        ts.metric == "ls"
        ts.attributes().size() == 2
        ts.attributesReference.size() == 2
        ts.attribute("prog") == "ls"
        ts.attribute("cmd") == "strace ls"
        ts.size() == 11
        ts.getTimestamps().size() == 11
        ts.getTime(0) == 0
        ts.getValues().size() == 11
        ts.getValue(0) == "line 0"
        //check array copy
        ts.getTimestampsAsArray().length == 11
        ts.getValuesAsArray().length == 11
        !ts.isEmpty()
    }

    def "test points"() {
        given:
        def times = new LongList()
        def values = new ArrayList<String>()
        10.times {
            times.add(100 - it as long)
            values.add(it * 10 as String)
        }
        def ts = new StraceTimeSeries.Builder("//CPU//Load").points(times, values).build()

        when:

        def stream = ts.points()
        then:
        stream.count() == 10

    }

    def "test sort"() {
        given:
        def times = []
        def values = []
        10.times {
            times.add(100 - it as long)
            values.add(100 - it as String)
        }
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()
        ts.addAll(times, values)

        when:
        ts.sort()

        then:
        ts.getValue(0) == "91"
    }

    def "test sort on empty time series"() {
        given:
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()

        when:
        ts.sort()

        then:
        0 * ts.points()
    }

    def "test clear time series"() {
        given:

        def times = new LongList()
        def values = new ArrayList<String>()

        10.times {
            times.add(it as long)
            values.add(it * 10 as String)
        }

        def ts = new StraceTimeSeries.Builder("//CPU//Load")
                .points(times, values)
                .build()

        when:
        ts.clear()

        then:
        ts.size() == 0
        ts.isEmpty()
        ts.getEnd() == 0
        ts.getStart() == 0
    }

    def "test to string"() {
        given:
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()

        when:
        def string = ts.toString()

        then:
        string.contains("metric")
    }

    def "test equals"() {
        given:
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()

        when:
        def result = ts.equals(other)

        then:
        result == expected

        where:
        other << [null, 1, new StraceTimeSeries.Builder("//CPU//Load").build()]
        expected << [false, false, true]
    }

    def "test equals same instance"() {
        given:
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()

        expect:
        ts.equals(ts)
    }

    def "test hash code"() {
        given:
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()
        def ts2 = new StraceTimeSeries.Builder("//CPU//Load").build()
        def ts3 = new StraceTimeSeries.Builder("//CPU//Load//").build()

        expect:
        ts.hashCode() == ts2.hashCode()
        ts.hashCode() != ts3.hashCode()
    }

    def "test empty points"() {
        expect:
        def ts = new StraceTimeSeries.Builder("").build()
        ts.points().count() == 0l
        ts.isEmpty()
    }

    def "test add all as array"() {
        given:
        def times = []
        def values = []
        10.times {
            times.add(100 - it as long)
            values.add(100 - it as String)
        }
        def ts = new StraceTimeSeries.Builder("//CPU//Load").build()
        ts.addAll(times as long[], values as String[])

        when:
        ts.sort()

        then:
        ts.getValue(0) == "91"
    }

    def "test attribute reference"() {
        given:
        def ts = new StraceTimeSeries.Builder("//CPU//Load")
                .attribute("added via builder", "oh dear")
                .build()
        when:
        ts.getAttributesReference().clear()

        then:
        ts.attributes().size() == 0
    }

}
