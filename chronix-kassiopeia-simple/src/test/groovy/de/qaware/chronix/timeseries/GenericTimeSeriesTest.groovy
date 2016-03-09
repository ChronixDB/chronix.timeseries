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

import de.qaware.chronix.timeseries.dt.GenericPoint
import spock.lang.Specification

import java.util.stream.Collectors

/**
 * Unit test for the generic time series
 * @author f.lautenschlager
 */
class GenericTimeSeriesTest extends Specification {

    def "test sort"() {
        given:
        def genericTimeSeries = new GenericTimeSeries.Builder<String>("test-generic-time-series")
                .point(5, "five")
                .point(3, "three")
                .point(6, "six")
                .build()
        when:
        genericTimeSeries.sort();

        then:
        List<GenericPoint<String>> sortedPoints = genericTimeSeries.points().collect(Collectors.toList())
        sortedPoints.get(0).value == "three"
        sortedPoints.get(1).value == "five"
        sortedPoints.get(2).value == "six"
    }
    /*

    def "test attribute"() {
        given:

        when:
        // TODO implement stimulus
        then:
        // TODO implement assertions
    }

    def "test attributes"() {
        given:

        when:
        // TODO implement stimulus
        then:
        // TODO implement assertions
    }

    def "test points"() {
        given:

        when:
        // TODO implement stimulus
        then:
        // TODO implement assertions
    }
    */
}
