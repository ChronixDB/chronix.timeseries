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
package de.qaware.chronix.timeseries

import spock.lang.Specification

/**
 * Unit test for the metric time series
 * @author f.lautenschlager
 */
class MetricTimeSeriesTest extends Specification {

    def "test create a metric time series and access its values"() {

        given:
        def times = []
        def values = []
        10.times {
            times.add(it as long)
            values.add(it * 10 as double)
        }
        def attributes = new HashMap<String, Object>()
        attributes.put("thread", 2 as long)

        when:
        def ts = new MetricTimeSeries.Builder("//CPU//Load")
                .attributes(attributes)
                .attribute("host", "laptop")
                .attribute("avg", 2.23)
                .data(times, values)
                .point(10 as long, 100)
                .build();

        then:
        ts.start == 0
        ts.end == 10
        ts.metric == "//CPU//Load"
        ts.attributes().size() == 3
        ts.attribute("host") == "laptop"
        ts.attribute("avg") == 2.23
        ts.attribute("thread") == 2 as long
        ts.size() == 11
        ts.getTimestamps().count() == 11
        ts.get(0) == 0
    }

    def "test pairs"() {
        given:
        def times = []
        def values = []
        10.times {
            times.add(100 - it as long)
            values.add(it * 10 as double)
        }
        def ts = new MetricTimeSeries.Builder("//CPU//Load").data(times, values).build();

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
            values.add(100 - it as double)
        }
        def ts = new MetricTimeSeries.Builder("//CPU//Load").data(times, values).build();

        when:
        ts.sort()

        then:
        ts.get(0) == 91
    }

    def "test clear time series"() {
        given:

        def times = []
        def values = []

        10.times {
            times.add(it as long)
            values.add(it * 10 as double)
        }

        def ts = new MetricTimeSeries.Builder("//CPU//Load")
                .data(times, values)
                .build();

        when:
        ts.clear()

        then:
        ts.size() == 0
        ts.empty()
    }

    def "test to string"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load").build()

        when:
        def string = ts.toString()

        then:
        string.contains("metric")
    }

    def "test equals"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load").build()

        when:
        def result = ts.equals(other)

        then:
        result == expected

        where:
        other << [null, 1, new MetricTimeSeries.Builder("//CPU//Load").build()]
        expected << [false, false, true]
    }

    def "test equals same instance"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load").build()

        expect:
        ts.equals(ts)
    }

    def "test hash code"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load").build()
        def ts2 = new MetricTimeSeries.Builder("//CPU//Load").build()
        def ts3 = new MetricTimeSeries.Builder("//CPU//Load//").build()

        expect:
        ts.hashCode() == ts2.hashCode()
        ts.hashCode() != ts3.hashCode()
    }

}
