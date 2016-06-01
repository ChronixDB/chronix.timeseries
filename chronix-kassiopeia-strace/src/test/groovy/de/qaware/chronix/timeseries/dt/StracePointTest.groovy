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
package de.qaware.chronix.timeseries.dt

import spock.lang.Specification

/**
 * Unit test for the strace-point class
 */
class StracePointTest extends Specification {

    def "test point"() {
        when:
        def pair = new StracePoint(0 as int, 1 as long, 2 as String)

        then:
        pair.index == 0
        pair.timestamp == 1l
        pair.value == "2"
    }

    def "test equals"() {
        when:
        def pair = new StracePoint(0i, 1l, "2")

        then:
        pair.equals(pair)
        pair.equals(other) == result

        where:
        other << [null, new Object(), new StracePoint(1i, 1l, "2"), new StracePoint(0i, 2l, "2"), new StracePoint(0i, 1l, "3"), new StracePoint(0i, 1l, "2")]
        result << [false, false, false, false, false, true]
    }

    def "test hashCode"() {
        when:
        def pair = new StracePoint(0i, 1l, "2")

        then:
        (pair.hashCode() == other.hashCode()) == result


        where:
        other << [new Object(), new StracePoint(1i, 1l, "2"), new StracePoint(0i, 2l, "2"), new StracePoint(0i, 1l, "3"), new StracePoint(0i, 1l, "2")]
        result << [false, false, false, false, true]
    }

    def "test to string"() {
        expect:
        new StracePoint(0i, 1l, "2").toString()
    }
}
