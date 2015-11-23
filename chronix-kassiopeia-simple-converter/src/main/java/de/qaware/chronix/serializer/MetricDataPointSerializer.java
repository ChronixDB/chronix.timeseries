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

import com.google.gson.*;
import de.qaware.chronix.dts.MetricDataPoint;

import java.lang.reflect.Type;

/**
 * Serializer for a metric data point.
 * A metric data point is represented as an array [date,value]
 *
 * @author f.lautenschlager
 */
public class MetricDataPointSerializer implements JsonSerializer<MetricDataPoint>, JsonDeserializer<MetricDataPoint> {

    @Override
    public JsonElement serialize(MetricDataPoint src, Type typeOfSrc, JsonSerializationContext context) {
        JsonArray result = new JsonArray();

        result.add(src.getDate());
        result.add(src.getValue());

        return result;
    }

    @Override
    public MetricDataPoint deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonArray point = json.getAsJsonArray();

        long date = point.get(0).getAsLong();
        double value = point.get(1).getAsDouble();

        return new MetricDataPoint(date, value);
    }
}
