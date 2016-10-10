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
package de.qaware.chronix.converter;

import de.qaware.chronix.timeseries.MetricTimeSeries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Holds default group by and reduce functions for the kassiopeia-simple time series
 *
 * @author f.lautenschlager
 */
public final class KassiopeiaSimpleDefaults {

    private KassiopeiaSimpleDefaults() {
        //avoid instances
    }


    /**
     * Default group by function for the metric time series class.
     * Groups time series on its metric name.
     */
    public static final Function<MetricTimeSeries, String> GROUP_BY = MetricTimeSeries::getMetric;

    /**
     * Default reduce function.
     * Attributes in both collected and reduced are merged using set holding both values.
     */
    public static final BinaryOperator<MetricTimeSeries> REDUCE = (collected, reduced) -> {
        collected.addAll(reduced.getTimestampsAsArray(), reduced.getValuesAsArray());

        //The collected attributes
        Map<String, Object> attributesReference = collected.getAttributesReference();

        //merge the attributes
        //we iterate over the copy
        for (HashMap.Entry<String, Object> entry : reduced.attributes().entrySet()) {
            //Attribute to add
            String attributeToAdd = entry.getKey();
            Set<Object> set = new HashSet<>();

            if (attributesReference.containsKey(attributeToAdd)) {
                Object value = attributesReference.get(attributeToAdd);
                set.add(value);
            }
            set.add(entry.getValue());
            attributesReference.put(attributeToAdd, set);
        }
        return collected;
    };
}