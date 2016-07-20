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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The abstract time series that bundles all the commonly used time series stuff.
 *
 * @author f.lautenschlager
 */
public abstract class AbstractTimeSeries implements Serializable {

    protected static final long serialVersionUID = 5497398456431471102L;
    protected long end;
    protected long start;
    String metric;
    LongList timestamps;
    Map<String, Object> attributes = new HashMap<>();

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
     * Gets the timestamp at the given index
     *
     * @param i the index position of the time stamp
     * @return the timestamp as long
     */
    public long getTime(int i) {
        return timestamps.get(i);
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
     * @return the size
     */
    public int size() {
        return timestamps.size();
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
    void addAttribute(String key, Object value) {
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
     * @return empty if the time series contains no points
     */
    public boolean isEmpty() {
        return timestamps.size() == 0;
    }


}
