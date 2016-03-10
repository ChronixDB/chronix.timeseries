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
package de.qaware.chronix.timeseries.dt;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A generic point of timestamp and value
 *
 * @param <T> the type of the value
 * @author f.lautenschlager
 */
public class GenericPoint<T> {
    private int index;
    private long timestamp;
    private T value;


    /**
     * Constructs a pair
     *
     * @param index     - the index of timestamp / value within the metric time series
     * @param timestamp - the timestamp
     * @param value     - the value
     */
    public GenericPoint(int index, long timestamp, T value) {
        this.index = index;
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * @return the index of within the time series
     */
    public int getIndex() {
        return index;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the generic value
     */
    public T getValue() {
        return value;
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
        GenericPoint rhs = (GenericPoint) obj;
        return new EqualsBuilder()
                .append(this.index, rhs.index)
                .append(this.timestamp, rhs.timestamp)
                .append(this.value, rhs.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(index)
                .append(timestamp)
                .append(value)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("index", index)
                .append("timestamp", timestamp)
                .append("value", value)
                .toString();
    }
}
