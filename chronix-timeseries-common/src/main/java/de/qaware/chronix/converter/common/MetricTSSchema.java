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
package de.qaware.chronix.converter.common;


import de.qaware.chronix.Schema;

/**
 * The metric time series schema.
 * It extends the default schema by a field called metric
 *
 * @author f.lautenschlager
 */
public final class MetricTSSchema {
    /**
     * The Metric field
     */
    public static final String METRIC = "metric";


    private MetricTSSchema() {
        //Private constructor
    }

    /**
     * Checks if the given field is a user defined.
     * This means that the field name is not one of the following
     * (id, start, end, data, metric)
     *
     * @param field - the field key
     * @return true if the field is user defined
     */
    public static boolean isUserDefined(String field) {
        return !METRIC.equals(field) && Schema.isUserDefined(field);
    }
}
