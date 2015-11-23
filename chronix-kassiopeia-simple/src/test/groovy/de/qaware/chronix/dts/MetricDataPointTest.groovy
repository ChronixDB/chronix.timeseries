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
package de.qaware.chronix.dts

import spock.lang.Specification

/**
 * Created by f.lautenschlager on 23.11.2015.
 */
class MetricDataPointTest extends Specification {

    def "test get value and date"() {
        when:
        def point = new MetricDataPoint(475896, 45678)

        then:
        point.getDate() == 475896
        point.getValue() == 45678
    }


    def "test compareTo"() {
        given:
        def pointOne = new MetricDataPoint(0, 1)
        def pointTwo = new MetricDataPoint(1, 1)

        when:
        def result = pointOne.compareTo(pointTwo)

        then:
        result == -1
    }

    def "test equals"() {
        given:
        def point = new MetricDataPoint(475896, 45678)

        when:
        def result = point.equals(other)

        then:
        result == expected

        where:
        other << [null, 1, new MetricDataPoint(475896, 45678)]
        expected << [false, false, true]
    }

    def "test equals same instance"() {
        given:
        def metricDataPoint = new MetricDataPoint(475896, 4711)
        expect:
        metricDataPoint.equals(metricDataPoint)
    }

    def "test toString"() {
        given:
        def point = new MetricDataPoint(475896, 45678)

        when:
        def string = point.toString()

        then:
        string.contains("date")
        string.contains("value")
    }
}
