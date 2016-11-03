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

import de.qaware.chronix.converter.common.DoubleList
import de.qaware.chronix.converter.common.LongList
import spock.lang.Specification

/**
 * Unit test for the metric time series
 * @author f.lautenschlager
 */
class MetricGenericTimeSeriesMaxTest extends Specification {

    def "test max"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        11.times {
            times.add(100 - it as long)
            values.add(it * 10 as double)
        }
        def ts = new MetricTimeSeries.Builder("SimpleMax").points(times, values).build()

        when:

        def max = ts.max()
        then:
        max == 100

    }

    def "test max with only NaN"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        11.times {
            times.add(100 - it as long)
            values.add(Double.NaN);
        }
        times.add(12 as long);
        values.add(Double.NaN);
        def ts = new MetricTimeSeries.Builder("SimpleMax").points(times, values).build()

        when:

        def max = ts.max()

        then:
        max == Double.MIN_VALUE;

    }

    def "test execute"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Max");
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        timeSeries.point(12, -10)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.max()
        then:
        result == 9999.0
    }

    def "test max for empty time series"() {
        when:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Max");

        def result = timeSeries.build().max();
        then:
        result == Double.NaN
    }
}
