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
 * Unit test for the generic point test
 * @author f.lautenschlager
 */
class GenericPointTest extends Specification {

    def "test create generic point and getters"() {
        given:
        def genericPoint = new GenericPoint<String>(0, 1076723468l, "JobScheduler FJ pool 6/8 #80 daemon java.lang.Thread.State: RUNNABLE")
        expect:
        genericPoint.index == 0
        genericPoint.timestamp == 1076723468l
        genericPoint.value == "JobScheduler FJ pool 6/8 #80 daemon java.lang.Thread.State: RUNNABLE"
    }
}
