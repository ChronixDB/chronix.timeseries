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


import de.qaware.chronix.converter.common.LongList;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * A metric time series that have at least the following fields:
 * - metric name,
 * - start and end,
 * - arbitrary attributes
 * and a list of metric data points (timestamp, double value)
 *
 * @author f.lautenschlager
 */
public final class LsofTimeSeries implements Serializable {

    private static final long serialVersionUID = 5497398456431471102L;

    private String metric;
    private LongList timestamps;
    private List<List<Lsof>> values;

    private Map<String, Object> attributes = new HashMap<>();
    private long end;
    private long start;

    /**
     * Private constructor.
     * To instantiate a metric time series use the builder class.
     */
    private LsofTimeSeries() {
        timestamps = new LongList(500);
        values = new ArrayList<>(500);
    }

    /**
     * Sets the start and end end based on the
     */
    private void setStartAndEnd() {
        //When the time stamps are empty we do not set the start and end
        //An aggregation or analysis response does not have a data field per default.
        if (!timestamps.isEmpty()) {
            start = timestamps.get(0);
            end = timestamps.get(size() - 1);
        }
    }

    /**
     * @return a copy of the metric data points
     */
    public LongList getTimestamps() {
        return timestamps.copy();
    }

    /**
     * In some cases if one just want to access all values,
     * that method is faster than {@see getTimestamps} due to no {@see LongList} initialization.
     *
     * @return a copy of the timestamps as array
     */
    public long[] getTimestampsAsArray() {
        return timestamps.toArray();
    }

    /**
     * @return a copy of the metric data points
     */
    public List<List<Lsof>> getValues() {
        return new ArrayList<>(values);
    }

    /**
     * In some cases if one just want to access all values,
     * that method is faster than {@see getValues} due to no {@see DoubleList} initialization.
     *
     * @return a copy of the values as array
     */
    public Lsof[] getValuesAsArray() {
        return (Lsof[]) values.toArray();
    }

    /**
     * Gets the metric data point at the index i
     *
     * @param i the index position of the metric value
     * @return the metric value
     */
    public List<Lsof> getValue(int i) {
        return values.get(i);
    }

    /**
     * Gets the timestamp at the given index
     *
     * @param i the index position of the time stamp
     * @return the timestamp as long
     */
    public long getTime(int i) {
        return timestamps.get(i);
    }

    /**
     * Sorts the time series values.
     */
    public void sort() {
        if (timestamps.size() > 1) {

            LongList sortedTimes = new LongList(timestamps.size());
            List<List<Lsof>> sortedValues = new ArrayList<>(values.size());

            points().sorted((o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp())).forEachOrdered(p -> {
                sortedTimes.add(p.getTimestamp());
                sortedValues.add(p.getValue());
            });

            timestamps = sortedTimes;
            values = sortedValues;
        }
    }

    /**
     * A stream over the points
     *
     * @return the points as points
     */
    public Stream<LsofPoint> points() {
        if (timestamps.isEmpty()) {
            return Stream.empty();
        }
        return Stream.iterate(of(0), pair -> of(pair.getIndex() + 1)).limit(timestamps.size());
    }

    private LsofPoint of(int index) {
        return new LsofPoint(index, timestamps.get(index), values.get(index));
    }


    /**
     * Sets the timestamps and values as data
     *
     * @param timestamps - the timestamps
     * @param values     - the values
     */
    private void setAll(LongList timestamps, List<List<Lsof>> values) {
        this.timestamps = timestamps;
        this.values = values;
    }

    /**
     * Adds all the given points to the time series
     *
     * @param timestamps the timestamps
     * @param values     the values
     */
    public final void addAll(LongList timestamps, List<List<Lsof>> values) {
        for (int i = 0; i < timestamps.size(); i++) {
            add(timestamps.get(i), values.get(i));
        }
    }


    /**
     * Adds a single timestamp and value
     *
     * @param timestamp the timestamp
     * @param value     the value
     */
    public final void add(long timestamp, List<Lsof> value) {
        this.timestamps.add(timestamp);
        this.values.add(value);
    }

    /**
     * @return the metric name
     */
    public String getMetric() {
        return metric;
    }

    /**
     * Adds an attribute to the time series
     *
     * @param key   the key
     * @param value the value
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
    public Object attribute(String key) {
        return attributes.get(key);
    }

    /**
     * @return a copy of the attributes of this time series
     */
    public Map<String, Object> attributes() {
        return new HashMap<>(attributes);
    }

    /**
     * This method should be used with care as it delivers the reference.
     *
     * @return the attributes of this time series
     */
    @SuppressWarnings("all")
    public Map<String, Object> getAttributesReference() {
        return attributes;
    }

    /**
     * Clears the time series
     */
    public void clear() {
        timestamps.clear();
        values.clear();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        LsofTimeSeries rhs = (LsofTimeSeries) obj;
        return new EqualsBuilder()
                .append(this.getMetric(), rhs.getMetric())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(getMetric())
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("metric", metric)
                .append("attributes", attributes)
                .toString();
    }


    /**
     * @return the start of the time series
     */
    public long getStart() {
        setStartAndEnd();
        return start;
    }

    /**
     * @return the end of the time series
     */
    public long getEnd() {
        setStartAndEnd();
        return end;
    }

    /**
     * @return the size
     */
    public int size() {
        return timestamps.size();
    }

    /**
     * @return empty if the time series contains no points
     */
    public boolean isEmpty() {
        return timestamps.size() == 0;
    }

    /**
     * @param field the field to group the values
     * @return a map with keys = values, value = set of lsof entries
     */
    public Map<String, List<Lsof>> groupBy(String field) {

        Map<String, List<Lsof>> result = new ConcurrentHashMap<>();

        values.parallelStream().forEach(value -> {
            //iterate over the concrete values
            for (Lsof lsof : value) {

                String fieldValue;

                switch (field) {
                    case "node":
                        fieldValue = lsof.getNode();
                        break;
                    case "name":
                        fieldValue = lsof.getName();
                        break;
                    case "user":
                        fieldValue = lsof.getUser();
                        break;
                    case "fd":
                        fieldValue = lsof.getFd();
                        break;
                    case "type":
                        fieldValue = lsof.getType();
                        break;
                    case "device":
                        fieldValue = lsof.getDevice();
                        break;
                    case "size":
                        fieldValue = lsof.getSize();
                        break;
                    default:
                        fieldValue = "*";
                        break;
                }

                if (!result.containsKey(fieldValue)) {
                    result.put(fieldValue, new ArrayList<>());
                }

                result.get(fieldValue).add(lsof);
            }
        });
        return result;
    }

    /**
     * The Builder class
     */
    public static final class Builder {

        /**
         * The time series object
         */
        private LsofTimeSeries lsofTimeS;

        /**
         * Constructs a new Builder
         *
         * @param metric the metric name
         */
        public Builder(String metric) {
            lsofTimeS = new LsofTimeSeries();
            lsofTimeS.metric = metric;
        }


        /**
         * @return the filled time series
         */
        public LsofTimeSeries build() {
            return lsofTimeS;
        }


        /**
         * Sets the time series data
         *
         * @param timestamps the time stamps
         * @param values     the values
         * @return the builder
         */
        public Builder points(LongList timestamps, List<List<Lsof>> values) {
            if (timestamps != null && values != null) {
                lsofTimeS.setAll(timestamps, values);
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
        public Builder point(long timestamp, List<Lsof> value) {
            lsofTimeS.timestamps.add(timestamp);
            lsofTimeS.values.add(value);
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
            lsofTimeS.addAttribute(key, value);
            return this;
        }

        /**
         * Sets the attributes for this time series
         *
         * @param attributes the time series attributes
         * @return the builder
         */
        public Builder attributes(Map<String, Object> attributes) {
            lsofTimeS.attributes = attributes;
            return this;
        }

        /**
         * Sets the end of the time series
         *
         * @param end the end of the time series
         * @return the builder
         */
        public Builder end(long end) {
            lsofTimeS.end = end;
            return this;
        }

        /**
         * Sets the start of the time series
         *
         * @param start the start of the time series
         * @return the builder
         */
        public Builder start(long start) {
            lsofTimeS.start = start;
            return this;
        }
    }
}
