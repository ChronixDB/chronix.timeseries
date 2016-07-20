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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A pair of timestamp and a string
 *
 * this class is somehow equivalent to de.qaware.chronix.timeseries.dt.NumericPoint
 *
 * @author Max Jalowski
 */
public class StringPoint {
    private int index;
    private long timestamp;
    private String value;

    /**
     * Constructs a pair
     *
     * @param index     - the index of timestamp / value within the metric time series
     * @param timestamp - the timestamp
     * @param value     - the value
     */
    public StringPoint(int index, long timestamp, String value) {
        this.index = index;
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * @return the index
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
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null){
            return false;
        }
        if (o == this){
            return true;
        }
        if (o.getClass() != getClass()){
            return false;
        }
        StringPoint sp = (StringPoint) o;
        return new EqualsBuilder()
                .append(this.index, sp.index)
                .append(this.timestamp, sp.timestamp)
                .append(this.value, sp.value)
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
