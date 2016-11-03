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
 * @author c.hillmann
 */
class MetricGenericTimeSeriesAvgTest extends Specification {


    def "test avg"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        times.add(1 as long)
        values.add(2)
        times.add(8 as long)
        values.add(3)
        times.add(3 as long)
        values.add(7)
        times.add(2 as long)
        values.add(8)

        def ts = new MetricTimeSeries.Builder("SimpleMax").points(times, values).build()

        when:

        def avg = ts.avg()
        then:
        avg == 5d

    }

    def "test avg 2"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Avg");
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.avg();
        then:
        result == 949.9090909090909d
    }

    def "test avg for empty time series"() {
        when:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Avg");

        def result = timeSeries.build().avg();
        then:
        result == Double.NaN
    }


}
