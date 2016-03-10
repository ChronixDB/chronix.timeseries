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
import de.qaware.chronix.timeseries.dt.LongList
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

    def "test sort with empty time series"() {
        given:
        def genericTimeSeries = new GenericTimeSeries.Builder<String>("test-generic-time-series").build()
        when:
        genericTimeSeries.sort();

        then:
        noExceptionThrown()
    }


    def "test attribute"() {
        when:
        def testSeries = genericTimeSeries;

        then:
        testSeries.getAttribute(contains) == result

        where:
        genericTimeSeries << [new GenericTimeSeries.Builder<String>("test-generic-time-series").build(),
                              new GenericTimeSeries.Builder<String>("test-generic-time-series")
                                      .attribute("something", "strange")
                                      .build()]
        contains << ["", "something"]
        result << [null, "strange"]
    }

    def "test attributes"() {
        when:
        def testSeries = genericTimeSeries

        then:
        //Check if we have a attributes copy
        testSeries.getAttributes().clear()
        testSeries.getAttributes().size() == size

        where:
        genericTimeSeries << [new GenericTimeSeries.Builder<String>("test-generic-time-series").build(),
                              new GenericTimeSeries.Builder<String>("test-generic-time-series")
                                      .attributes(["key": "value"])
                                      .build()]
        size << [0, 1]
    }

    def "test points with start and end"() {
        when:
        def testSeries = genericTimeSeries

        then:
        def result = testSeries.points().collect(Collectors.toList())
        result.size() == size
        result.containsAll(contains)

        testSeries.getStart() == start
        testSeries.getEnd() == end

        where:
        genericTimeSeries << [new GenericTimeSeries.Builder<String>("test-generic-time-series").build(),
                              new GenericTimeSeries.Builder<String>("test-generic-time-series")
                                      .point(0l, "Some fancy Value")
                                      .point(1l, "A even more fancy Value")
                                      .build(),
                              new GenericTimeSeries.Builder<String>("test-generic-time-series")
                                      .points(generateLongList(10), generateValues(10))
                                      .build()]
        size << [0, 2, 10]
        contains << [[], [new GenericPoint<String>(0i, 0l, "Some fancy Value",),
                          new GenericPoint<String>(1i, 1l, "A even more fancy Value")],
                     generatePoints(10)]
        start << [0, 0, 0]
        end << [0, 1, 9]
    }

    def List<GenericPoint<String>> generatePoints(int i) {
        def result = []
        i.times {
            result.add(new GenericPoint<>(it, it, "value-" + it))
        }
        return result
    }

    List<String> generateValues(int i) {
        def list = []
        i.times {
            list.add("value-" + it)
        }
        return list
    }

    LongList generateLongList(int i) {
        def list = new LongList(i)
        i.times {
            list.add(it)
        }
        return list

    }

    def "test to string"() {
        expect:
        new GenericTimeSeries.Builder<String>("something").toString()
    }

}
