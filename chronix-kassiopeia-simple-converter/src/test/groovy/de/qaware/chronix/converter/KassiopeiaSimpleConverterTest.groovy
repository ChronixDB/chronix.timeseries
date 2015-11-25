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
package de.qaware.chronix.converter

import de.qaware.chronix.dts.MetricDataPoint
import de.qaware.chronix.timeseries.MetricTimeSeries
import spock.lang.Specification

/**
 * Unit test for the kassiopeia simple converter
 * @author f.lautenschlager
 */
class KassiopeiaSimpleConverterTest extends Specification {

    def "test to and from"() {
        given:
        def ts = new MetricTimeSeries.Builder("\\Load\\avg")
                .start(0)
                .end(100)
                .attribute("MyField", 4711)
                .build();

        100.times {
            ts.add(new MetricDataPoint(it, it * 2))
        }

        def converter = new KassiopeiaSimpleConverter();

        when:
        def binaryDocument = converter.to(ts)
        def tsReconverted = converter.from(binaryDocument, 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 100
        tsReconverted.get(1).date == 1
        tsReconverted.get(1).value == 2
        tsReconverted.attribute("MyField") == 4711

    }
}
