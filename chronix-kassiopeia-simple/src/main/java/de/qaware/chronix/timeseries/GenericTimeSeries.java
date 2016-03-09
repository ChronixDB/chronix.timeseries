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
package de.qaware.chronix.timeseries;

import de.qaware.chronix.timeseries.dt.GenericPoint;
import de.qaware.chronix.timeseries.dt.LongList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


/**
 * A time series of pairs with timestamp and generic value
 *
 * @param <T> the type of the value
 * @author f.lautenschlager
 */
public class GenericTimeSeries<T> {

    //Metric or name, however we should have a identifier
    private String metric;
    private LongList timestamps;
    private List<T> values;

    private Map<String, Object> attributes = new HashMap<>();


    /**
     * Constructs a generic time series with timestamps as long
     * and a values of type t.
     */
    private GenericTimeSeries() {
        timestamps = new LongList(500);
        values = new ArrayList<>();
    }

    /**
     * @return the start of the time series
     */
    public long getStart() {
        if (timestamps.isEmpty()) {
            return 0;
        } else {
            return timestamps.get(0);
        }
    }

    /**
     * @return the end of the time series
     */
    public long getEnd() {
        if (timestamps.isEmpty()) {
            return 0;
        } else {
            return timestamps.get(timestamps.size() - 1);
        }
    }

    /**
     * Sorts the timestamps of the time series in ascending order.
     */
    public void sort() {
        if (timestamps.size() > 1) {

            LongList sortedTimes = new LongList(timestamps.size());
            List<T> sortedValues = new ArrayList<>(timestamps.size());

            points().sorted((o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp())).forEachOrdered(p -> {
                sortedTimes.add(p.getTimestamp());
                sortedValues.add(p.getValue());
            });

            timestamps = sortedTimes;
            values = sortedValues;
        }
    }

    /**
     * Adds the given attribute (key,value) to the time series
     *
     * @param key   the attribute name / key
     * @param value the value belonging to the attribute name
     */
    private void addAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * Get the attribute for the given key
     *
     * @param key the attribute key
     * @return the value as object
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * @return a copy of the attributes of this time series
     */
    public Map<String, Object> getAttributes() {
        return new HashMap<>(attributes);
    }

    /**
     * A stream over the points
     *
     * @return the points as points
     */
    public Stream<GenericPoint<T>> points() {
        if (timestamps.isEmpty()) {
            return Stream.empty();
        }
        return Stream.iterate(of(0), pair -> of(pair.getIndex() + 1)).limit(timestamps.size());
    }


    private GenericPoint<T> of(int index) {
        return new GenericPoint<>(index, timestamps.get(index), values.get(index));
    }

    /**
     * Sets the timestamps and values as data
     *
     * @param timestamps the timestamps
     * @param values     the values
     */
    private void setAll(LongList timestamps, List<T> values) {
        this.timestamps = timestamps;
        this.values = values;
    }


    /**
     * The Builder class
     */
    public static final class Builder<T> {

        /**
         * The time series object
         */
        private GenericTimeSeries<T> genericTimeSeries;

        /**
         * Constructs a new Builder
         *
         * @param metric the metric name
         */
        public Builder(String metric) {
            genericTimeSeries = new GenericTimeSeries<>();
            genericTimeSeries.metric = metric;
        }


        /**
         * @return the filled time series
         */
        public GenericTimeSeries<T> build() {
            return genericTimeSeries;
        }


        /**
         * Sets the time series data
         *
         * @param timestamps the time stamps
         * @param values     the values
         * @return the builder
         */
        public Builder points(LongList timestamps, List<T> values) {
            if (timestamps != null && values != null) {
                genericTimeSeries.setAll(timestamps, values);
            }
            return this;
        }

        /**
         * Adds the given single data point to the time series
         *
         * @param timestamp the timestamp of the value
         * @param value     the belonging value
         * @return the builder
         */
        public Builder point(long timestamp, T value) {
            genericTimeSeries.timestamps.add(timestamp);
            genericTimeSeries.values.add(value);
            return this;
        }

        /**
         * Adds an attribute to the class
         *
         * @param key   the name of the attribute
         * @param value the value of the attribute
         * @return the builder
         */
        public Builder attribute(String key, Object value) {
            genericTimeSeries.addAttribute(key, value);
            return this;
        }

        /**
         * Sets the attributes for this time series
         *
         * @param attributes the time series attributes
         * @return the builder
         */
        public Builder attributes(Map<String, Object> attributes) {
            genericTimeSeries.attributes = attributes;
            return this;
        }

    }
}
