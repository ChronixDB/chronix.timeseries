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
package de.qaware.chronix.serializer

import de.qaware.chronix.dts.MetricDataPoint
import spock.lang.Specification

/**
 * Unit test for the kassiopeia serializer
 * @author f.lautenschlager
 */
class JsonKassiopeiaSerializerTest extends Specification {

    def "test serialize to and deserialize from json"() {
        given:
        def metricDataPoints = [new MetricDataPoint(0l, 4711), new MetricDataPoint(1l, 8564), new MetricDataPoint(2l, 1237),]
        def serializer = new JsonKassiopeiaSimpleSerializer()

        when:
        def json = serializer.toJson(metricDataPoints)

        then:
        List<MetricDataPoint> deserialize = new ArrayList<>(serializer.fromJson(json))
        deserialize.get(0).date == 0l
        deserialize.get(0).value == 4711
        deserialize.get(2).date == 2l
        deserialize.get(2).value == 1237

    }
}
