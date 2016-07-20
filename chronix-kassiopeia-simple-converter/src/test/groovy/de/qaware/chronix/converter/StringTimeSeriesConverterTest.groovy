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
package de.qaware.chronix.converter

import de.qaware.chronix.timeseries.StringTimeSeries
import spock.lang.Specification

import java.time.Instant

/**
 * Unit test for the kassiopeia simple converter
 * @author f.lautenschlager & m.jalowski
 */
class StringTimeSeriesConverterTest extends Specification {

    def "test to and from compressed data"() {
        given:
        def ts = new StringTimeSeries.Builder("\\Load\\avg").attribute("MyField", "4711")
        def start = Instant.now()

        100.times {
            ts.point(start.plusSeconds(it).toEpochMilli(), it * 2 + " as string")
        }

        def converter = new StraceTimeSeriesConverter();

        when:
        def binaryTimeSeries = converter.to(ts.build())
        def tsReconverted = converter.from(binaryTimeSeries, start.toEpochMilli(), start.plusSeconds(20).toEpochMilli())

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 21
        tsReconverted.getValue(1) == "2 as string"
        tsReconverted.attribute("MyField") == "4711"

    }

    def "test to and from aggregated value"() {
        given:
        def converter = new StraceTimeSeriesConverter();

        def binTs = new BinaryTimeSeries.Builder()
                .field("function_value", "4711")
                .field("metric", "\\Load\\avg")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 1
        tsReconverted.getValue(0) == "4711"
        tsReconverted.start == 5
        tsReconverted.end == 5
    }

    def "test to and from json data"() {
        given:
        def converter = new StraceTimeSeriesConverter();

        def binTs = new BinaryTimeSeries.Builder()
                .field("function_value", "4711")
                .field("metric", "\\Load\\avg")
                .field("dataAsJson", "[[0,1,2,3],['string0','string1','string2','string3']]")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 4
        tsReconverted.getValue(3) == 'string3'
        tsReconverted.start == 0
        tsReconverted.end == 3
    }

    def "test to and from json data with encoding exception"() {
        given:
        def converter = new StraceTimeSeriesConverter();

        def binTs = new BinaryTimeSeries.Builder()
                .field("function_value", "4711")
                .field("metric", "\\Load\\avg")
                .field("dataAsJson", new String("[[0,1,2,3],['string0','string1','string2','string3']]".getBytes("IBM420")))
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.metric == "\\Load\\avg"
        tsReconverted.size() == 0
        tsReconverted.start == 0
        tsReconverted.end == 0
    }
}
