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
package de.qaware.chronix.timeseries.de

import de.qaware.chronix.timeseries.dt.Pair
import spock.lang.Specification

/**
 * Unit test for the pair class
 */
class PairTest extends Specification {

    def "test pair"() {
        when:
        def pair = new Pair(0 as int, 1 as long, 2 as double)

        then:
        pair.index == 0
        pair.timestamp == 1l
        pair.value == 2d
    }

}
