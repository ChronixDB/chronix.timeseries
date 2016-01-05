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
package de.qaware.chronix.converter.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The json serializer for the kassiopeia simple time series
 *
 * @author f.lautenschlager
 */
public class JsonKassiopeiaSimpleSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKassiopeiaSimpleSerializer.class);
    public static final String UTF_8 = "UTF-8";

    private final Gson gson;

    /**
     * Constructs a new JsonKassiopeiaSimpleSerializer.
     */
    public JsonKassiopeiaSimpleSerializer() {

        GsonBuilder gsonBuilder = new GsonBuilder();

        gson = gsonBuilder.create();
    }

    /**
     * Serializes the collection of metric data points to json
     *
     * @param timestamps -  the timestamps
     * @param values     -  the values
     * @return a json serialized collection of metric data points
     */
    public byte[] toJson(Stream<Long> timestamps, Stream<Double> values) {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(baos, UTF_8));
            List[] data = new List[]{timestamps.collect(Collectors.toList()), values.collect(Collectors.toList())};
            gson.toJson(data, List[].class, writer);

            writer.close();
            baos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Could not serialize data to json", e);
        }
        return new byte[]{};
    }

    /**
     * Deserialize the given json to a collection of metric data points
     *
     * @param json       - the json representation of collection holding metric data points
     * @param queryStart
     * @param queryEnd   @return a collection holding the metric data points
     */
    public List[] fromJson(byte[] json, final long queryStart, final long queryEnd) {
        if (queryStart <= 0 && queryEnd <= 0) {
            return new List[]{new ArrayList<>(), new ArrayList<>()};
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(json);
            JsonReader reader = new JsonReader(new InputStreamReader(bais, UTF_8));
            List[] timestampsValues = gson.fromJson(reader, List[].class);
            reader.close();

            List<Double> times = (List<Double>) timestampsValues[0];
            List<Double> values = (List<Double>) timestampsValues[1];

            List<Long> filteredTimes = new ArrayList<>();
            List<Double> filteredValues = new ArrayList<>();


            for (int i = 0; i < times.size(); i++) {
                if (times.get(i) > queryEnd) {
                    break;
                }

                if (times.get(i) >= queryStart && times.get(i) <= queryEnd) {
                    filteredTimes.add(times.get(i).longValue());
                    filteredValues.add(values.get(i));
                }
            }

            return new List[]{filteredTimes, filteredValues};

        } catch (IOException e) {
            LOGGER.error("Could not deserialize json data", e);
        }
        return new List[]{new ArrayList<>(), new ArrayList<>()};

    }

}
