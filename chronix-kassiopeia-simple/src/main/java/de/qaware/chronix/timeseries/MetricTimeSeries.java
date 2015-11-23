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

package de.qaware.chronix.timeseries;


import de.qaware.chronix.dts.MetricDataPoint;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

/**
 * A metric time series that have at least the following fields:
 * - metric name,
 * - start and end,
 * - arbitrary attributes
 * and a list of metric data points (timestamp, double value)
 *
 * @author f.lautenschlager
 */
public class MetricTimeSeries {

    private String metric;
    private List<MetricDataPoint> points;
    private long start;
    private long end;

    private Map<String, Object> attributes = new HashMap<>();

    /**
     * Private constructor.
     * To instantiate a metric time series use the builder class.
     */
    private MetricTimeSeries() {
        points = new ArrayList<>();
    }

    /**
     * @return a copy of the metric data points
     */
    public List<MetricDataPoint> getPoints() {
        return new ArrayList<>(points);
    }


    /**
     * Gets the metric data point at the index i
     *
     * @param i - the index position of the metric data point
     * @return the metric data point
     */
    public MetricDataPoint get(int i) {
        return points.get(i);
    }

    /**
     * Adds a point to the time series.
     * Sets the start or end date if necessary.
     *
     * @param pointToAdd - the point
     */
    public final void add(MetricDataPoint pointToAdd) {
        if (pointToAdd == null) {
            return;
        }
        long time = pointToAdd.getDate();
        if (empty()) {
            start = end = time;
        }
        boolean requireSort = setStartOrEndDate(time);
        points.add(pointToAdd);

        if (requireSort) {
            Collections.sort(points);
        }
    }


    /**
     * Sets the min an max date of this time series
     *
     * @param date - the date
     */
    private boolean setStartOrEndDate(long date) {
        boolean requireSort = false;

        if (start > date) {
            start = date;
            requireSort = true;
        }
        if (end < date) {
            end = date;
            requireSort = true;
        }

        return requireSort;
    }

    /**
     * Adds all the given points to the time series
     *
     * @param points - the points
     */
    public final void addAll(Collection<MetricDataPoint> points) {
        points.forEach(this::add);
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
     * @param key   - the key
     * @param value - the value
     */
    private void addAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * Get the attribute for the given key
     *
     * @param key - the attribute key
     * @return the value as object
     */
    public Object attribute(String key) {
        return attributes.get(key);
    }

    /**
     * @return a copy of the attribute of this time series
     */
    public Map<String, Object> attributes() {
        return new HashMap<>(attributes);
    }

    /**
     * Clears the time series
     */
    public void clear() {
        start = end = -1;
        points.clear();
    }


    /**
     * @return true if empty, otherwise false
     */
    public boolean empty() {
        return points.isEmpty();
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
        MetricTimeSeries rhs = (MetricTimeSeries) obj;
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
        final StringBuilder sb = new StringBuilder("");
        sb.append("metric='").append(getMetric()).append('\'');
        return sb.toString();
    }

    /**
     * @return the start of the time series
     */
    public long getStart() {
        return start;
    }

    /**
     * @return the end of the time series
     */
    public long getEnd() {
        return end;
    }


    /**
     * @return the size
     */
    public int size() {
        return points.size();
    }


    /**
     * The Builder class
     */
    public static final class Builder {

        /**
         * The time series object
         */
        private MetricTimeSeries metricTimeSeries;

        /**
         * Constructs a new Builder
         *
         * @param metric - the metric name
         */
        public Builder(String metric) {
            metricTimeSeries = new MetricTimeSeries();
            metricTimeSeries.metric = metric;
        }


        /**
         * @return the filled time series
         */
        public MetricTimeSeries build() {
            return metricTimeSeries;
        }


        /**
         * Adds the data (base64 encoded) to the current series
         *
         * @param points - the encoded data
         * @return the builder
         */
        public Builder data(Collection<MetricDataPoint> points) {
            metricTimeSeries.addAll(points);
            return this;
        }


        /**
         * Adds the start date to the current series
         *
         * @param startDate - the start date
         * @return the builder
         */
        public Builder start(long startDate) {
            metricTimeSeries.start = startDate;
            return this;
        }

        /**
         * Adds the start date to the current series
         *
         * @param endDate - the end date
         * @return the builder
         */
        public Builder end(long endDate) {
            metricTimeSeries.end = endDate;
            return this;
        }

        /**
         * Adds the given single data point to the time series
         *
         * @param metricDataPoint - a metric data point
         * @return the builder
         */
        public Builder point(MetricDataPoint metricDataPoint) {
            metricTimeSeries.add(metricDataPoint);
            return this;
        }

        /**
         * Adds an attribute to the class
         *
         * @param key   - the name of the attribute
         * @param value - the value of the attribute
         * @return the builder
         */
        public Builder attribute(String key, Object value) {
            metricTimeSeries.addAttribute(key, value);
            return this;
        }

    }


}
