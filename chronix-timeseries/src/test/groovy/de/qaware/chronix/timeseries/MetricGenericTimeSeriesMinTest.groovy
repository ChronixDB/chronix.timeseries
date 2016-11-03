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
class MetricGenericTimeSeriesMinTest extends Specification {


    def "test min"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        11.times {
            times.add(100 - it as long)
            values.add(it * 10 as double)
        }
        def ts = new MetricTimeSeries.Builder("SimpleMin").points(times, values).build()

        when:

        def min = ts.min()
        then:
        min == 0

    }

    def "test execute with negative"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Min");
        10.times {
            timeSeries.point(it, it * -10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.min()
        then:
        result == -90.0
    }

    def "test execute with positive number"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Min");
        10.times {
            timeSeries.point(it, it * 10 + 1)
        }
        timeSeries.point(11, 0)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.min()
        then:
        result == 0.0
    }

    def "test for empty time series"() {
        when:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Avg");

        def result = timeSeries.build().min()
        then:
        result == Double.NaN
    }


}
