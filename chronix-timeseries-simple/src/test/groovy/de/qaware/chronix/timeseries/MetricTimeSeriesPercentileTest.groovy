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

import spock.lang.Specification

/**
 * Created by c.hillmann on 07.04.2016.
 */
class MetricTimeSeriesPercentileTest extends Specification {
    def "test percentile"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("P");
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.percentile(0.5)
        then:
        result == 50.0
    }

    def "test for empty time series"() {
        when:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Empty");

        def result = timeSeries.build().percentile(0.5)
        then:
        result == Double.NaN
    }
}
