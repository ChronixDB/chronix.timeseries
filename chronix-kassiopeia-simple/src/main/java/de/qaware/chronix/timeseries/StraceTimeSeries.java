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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
public final class StraceTimeSeries implements Serializable {

    private static final long serialVersionUID = 5497398456431471102L;

    private String metric;
    private LongList timestamps;
    private List<Strace> values;

    private Map<String, Object> attributes = new HashMap<>();
    private long end;
    private long start;

    /**
     * Private constructor.
     * To instantiate a metric time series use the builder class.
     */
    private StraceTimeSeries() {
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
    public List<Strace> getValues() {
        return new ArrayList<>(values);
    }


    /**
     * Gets the metric data point at the index i
     *
     * @param i the index position of the metric value
     * @return the metric value
     */
    public Strace getValue(int i) {
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
            List<Strace> sortedValues = new ArrayList<>(values.size());

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
    public Stream<StracePoint> points() {
        if (timestamps.isEmpty()) {
            return Stream.empty();
        }
        return Stream.iterate(of(0), pair -> of(pair.getIndex() + 1)).limit(timestamps.size());
    }

    private StracePoint of(int index) {
        return new StracePoint(index, timestamps.get(index), values.get(index));
    }


    /**
     * Sets the timestamps and values as data
     *
     * @param timestamps - the timestamps
     * @param values     - the values
     */
    private void setAll(LongList timestamps, List<Strace> values) {
        this.timestamps = timestamps;
        this.values = values;
    }

    /**
     * Adds all the given points to the time series
     *
     * @param timestamps the timestamps
     * @param values     the values
     */
    public final void addAll(LongList timestamps, List<Strace> values) {
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
    public final void add(long timestamp, Strace value) {
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
        StraceTimeSeries rhs = (StraceTimeSeries) obj;
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


    public Collection<StraceTimeSeries> split() {
        Pattern valuePattern = Pattern.compile(".*\\((\\d+).*");

        Map<String, StraceTimeSeries> splits = new HashMap<>();

        for (int i = 0; i < values.size(); i++) {

            Strace value = values.get(i);

            Matcher matcher = valuePattern.matcher(value.getCall());
            if (matcher.matches()) {
                String groupValue = matcher.group(1);

                if (!splits.containsKey(groupValue)) {
                    splits.put(groupValue, new StraceTimeSeries.Builder(metric + "-" + groupValue).build());
                }

                splits.get(groupValue).add(timestamps.get(i), value);
            }
        }
        return splits.values();
    }

    /**
     * The Builder class
     */
    public static final class Builder {

        /**
         * The time series object
         */
        private StraceTimeSeries straceTimeSeries;

        /**
         * Constructs a new Builder
         *
         * @param metric the metric name
         */
        public Builder(String metric) {
            straceTimeSeries = new StraceTimeSeries();
            straceTimeSeries.metric = metric;
        }


        /**
         * @return the filled time series
         */
        public StraceTimeSeries build() {
            return straceTimeSeries;
        }


        /**
         * Sets the time series data
         *
         * @param timestamps the time stamps
         * @param values     the values
         * @return the builder
         */
        public Builder points(LongList timestamps, List<Strace> values) {
            if (timestamps != null && values != null) {
                straceTimeSeries.setAll(timestamps, values);
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
        public Builder point(long timestamp, Strace value) {
            straceTimeSeries.timestamps.add(timestamp);
            straceTimeSeries.values.add(value);
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
            straceTimeSeries.addAttribute(key, value);
            return this;
        }

        /**
         * Sets the attributes for this time series
         *
         * @param attributes the time series attributes
         * @return the builder
         */
        public Builder attributes(Map<String, Object> attributes) {
            straceTimeSeries.attributes = attributes;
            return this;
        }

        /**
         * Sets the end of the time series
         *
         * @param end the end of the time series
         * @return the builder
         */
        public Builder end(long end) {
            straceTimeSeries.end = end;
            return this;
        }

        /**
         * Sets the start of the time series
         *
         * @param start the start of the time series
         * @return the builder
         */
        public Builder start(long start) {
            straceTimeSeries.start = start;
            return this;
        }
    }
}
