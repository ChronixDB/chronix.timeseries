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
package de.qaware.chronix.converter.common

import spock.lang.Specification

/**
 * Unit test fot the metric time series schema
 * @author f.lautenschlager
 */
class MetricTSSchemaTest extends Specification {

    def "test private constructor"() {
        when:
        MetricTSSchema.newInstance()
        then:
        noExceptionThrown()
    }

    def "test is user defined attribute"() {
        when:
        def result = MetricTSSchema.isUserDefined(field)
        then:
        result == expected
        where:
        field << ["metric", "id", "start", "end", "data", "my field"]
        expected << [false, false, false, false, false, true]
    }
}
