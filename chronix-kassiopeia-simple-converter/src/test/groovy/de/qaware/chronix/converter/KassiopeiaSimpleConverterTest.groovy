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

import de.qaware.chronix.timeseries.MetricTimeSeries
import spock.lang.Specification

/**
 * Unit test for the kassiopeia simple converter
 * @author f.lautenschlager
 */
class KassiopeiaSimpleConverterTest extends Specification {

    def "test to and from compressed data"() {
        given:
        def ts = new MetricTimeSeries.Builder("\\Load\\avg")
                .attribute("MyField", 4711)

        100.times {
            ts.point(it, it * 2)
        }

        def converter = new KassiopeiaSimpleConverter();

        when:
        def binaryTimeSeries = converter.to(ts.build())
        def tsReconverted = converter.from(binaryTimeSeries, 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 100
        tsReconverted.get(1) == 2
        tsReconverted.attribute("MyField") == 4711

    }

    def "test to and from aggregated value"() {
        given:
        def converter = new KassiopeiaSimpleConverter();

        def binTs = new BinaryTimeSeries.Builder()
                .field("value", 4711d)
                .field("metric", "\\Load\\avg")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 1
        tsReconverted.get(0) == 4711d
        tsReconverted.start == 5
        tsReconverted.end == 5
    }

    def "test to and from uncompressed data"() {
        given:
        def converter = new KassiopeiaSimpleConverter();

        def binTs = new BinaryTimeSeries.Builder()
                .field("value", 4711d)
                .field("metric", "\\Load\\avg")
                .field("dataAsJson", "[[0,1,2,3],[4711.0,4712.0,4713.0,4714.0]]")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 4
        tsReconverted.get(3) == 4714d
        tsReconverted.start == 0
        tsReconverted.end == 3
    }
}
