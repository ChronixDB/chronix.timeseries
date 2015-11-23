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
package de.qaware.chronix.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import de.qaware.chronix.dts.MetricDataPoint;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

/**
 * The json serializer for the kassiopeia simple time series
 *
 * @author f.lautenschlager
 */
public class JsonKassiopeiaSimpleSerializer {

    private final Gson gson;
    private final Type listType;

    /**
     * Constructs a new JsonKassiopeiaSimpleSerializer.
     */
    public JsonKassiopeiaSimpleSerializer() {
        listType = new TypeToken<List<MetricDataPoint>>() {
        }.getType();

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(MetricDataPoint.class, new MetricDataPointSerializer());

        gson = gsonBuilder.create();
    }

    /**
     * Serializes the collection of metric data points to json
     *
     * @param points - the metric data points
     * @return a json serialized collection of metric data points
     */
    public String toJson(Collection<MetricDataPoint> points) {
        return gson.toJson(points);
    }

    /**
     * Deserialize the given json to a collection of metric data points
     *
     * @param json - the json representation of collection holding metric data points
     * @return a collection holding the metric data points
     */
    public Collection<MetricDataPoint> fromJson(String json) {
        return gson.fromJson(json, listType);
    }

}
