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

import com.google.common.io.Files
import de.qaware.chronix.timeseries.Lsof
import de.qaware.chronix.timeseries.LsofTimeSeries
import spock.lang.Ignore
import spock.lang.Specification

import java.nio.charset.Charset
import java.time.Instant

/**
 * Created by f.lautenschlager on 16.09.2016.
 */
class LsofTimeSeriesConverterTest extends Specification {

    @Ignore
    def "test from"() {
        given:

        def path = "B:\\\\codebase\\\\chronixDB-github\\\\chronix.kassiopeia\\\\chronix-kassiopeia-simple\\\\src\\\\test\\\\resources\\\\2015-08-12-17-56-24.lsof"

        def builder = new LsofTimeSeries.Builder("lsof");

        def lsofPoints = new HashSet<>();

        when:
        Files.readLines(new File(path), Charset.defaultCharset()).each { line ->

            if (!line.startsWith("COMMAND")) {
                def lsof = parse(line)
                lsofPoints.add(lsof)
            }
        };
        builder.point(Instant.now().toEpochMilli(), lsofPoints);


        then:
        def ts = builder.build()
        new LsofTimeSeriesConverter().to(ts)
        noExceptionThrown()
    }


    def Lsof parse(String line) {
        def lsof = new Lsof.Builder();
        def splits = line.split(" ")

        def onlyValues = removeEmptySplits(splits) as ArrayList<String>

        lsof.command(onlyValues.get(0))
        lsof.pid(onlyValues.get(1) as int)
        lsof.user(onlyValues.get(2))
        lsof.fd(onlyValues.get(3))
        lsof.type(onlyValues.get(4))
        lsof.device(onlyValues.get(5))
        lsof.size(onlyValues.get(6))
        lsof.node(onlyValues.get(7))
        lsof.name(onlyValues.get(8))

        lsof.build();

    }

    def removeEmptySplits(String[] strings) {
        def values = []
        for (def split : strings) {
            if (!split.isEmpty()) {
                values.add(split)
            }
        }
        values
    }

}
