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

package de.qaware.chronix.dts;


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A metric data point that extends our data point with an double value.
 *
 * @author f.lautenschlager
 */
public class MetricDataPoint implements Comparable<MetricDataPoint> {

    private final long date;
    /**
     * The default value
     */
    private final double value;

    /**
     * Constructs a point from the given arguments
     *
     * @param date - the x value as timestamp
     * @param y    - the y value as double
     */
    public MetricDataPoint(final long date, final double y) {
        this.date = date;
        this.value = y;
    }

    /**
     * @return the numerical value of this point
     */
    public double getValue() {
        return value;
    }

    /**
     * @return the date
     */
    public long getDate() {
        return date;
    }


    @Override
    public int compareTo(MetricDataPoint o) {
        return Long.compare(date, o.date);
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
        MetricDataPoint rhs = (MetricDataPoint) obj;
        return new EqualsBuilder()
                .append(this.date, rhs.date)
                .append(this.value, rhs.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(date)
                .append(value)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("date", date)
                .append("value", value)
                .toString();
    }
}
