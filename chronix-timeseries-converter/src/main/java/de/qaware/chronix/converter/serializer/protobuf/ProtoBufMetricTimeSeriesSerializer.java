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
package de.qaware.chronix.converter.serializer.protobuf;


import de.qaware.chronix.converter.common.DoubleList;
import de.qaware.chronix.converter.common.LongList;
import de.qaware.chronix.converter.serializer.gen.MetricProtocolBuffers;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import de.qaware.chronix.timeseries.dts.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class to easily convert the protocol buffer into Point<Long,Double>
 *
 * @author f.lautenschlager
 */
public final class ProtoBufMetricTimeSeriesSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufMetricTimeSeriesSerializer.class);

    /**
     * Private constructor
     */
    private ProtoBufMetricTimeSeriesSerializer() {
        //utility class
    }

    /**
     * Add the points to the given builder
     *
     * @param decompressedBytes the decompressed input stream
     * @param timeSeriesStart   start of the time series
     * @param timeSeriesEnd     end of the time series
     * @param builder           the builder
     */
    public static void from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, MetricTimeSeries.Builder builder) {
        from(decompressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, builder);
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     * @param from              including points from
     * @param to                including points to
     * @param builder           the time series builder
     */
    public static void from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, MetricTimeSeries.Builder builder) {
        if (from == -1 || to == -1) {
            throw new IllegalArgumentException("FROM or TO have to be >= 0");
        }

        //if to is left of the time series, we have no points to return
        if (to < timeSeriesStart) {
            return;
        }
        //if from is greater  to, we have nothing to return
        if (from > to) {
            return;
        }

        //if from is right of the time series we have nothing to return
        if (from > timeSeriesEnd) {
            return;
        }

        try {
            MetricProtocolBuffers.Points protocolBufferPoints = MetricProtocolBuffers.Points.parseFrom(decompressedBytes);

            List<MetricProtocolBuffers.Point> pList = protocolBufferPoints.getPList();

            int size = pList.size();
            MetricProtocolBuffers.Point[] points = pList.toArray(new MetricProtocolBuffers.Point[0]);

            long[] timestamps = new long[pList.size()];
            double[] values = new double[pList.size()];

            long lastOffset = protocolBufferPoints.getDdc();
            long calculatedPointDate = timeSeriesStart;
            int lastPointIndex = 0;

            double value;

            for (int i = 0; i < size; i++) {
                MetricProtocolBuffers.Point p = points[i];

                //Decode the time
                if (i > 0) {
                    lastOffset = calculatePoint(p, lastOffset);
                    calculatedPointDate += lastOffset;
                }

                //only add the point if it is within the date
                if (calculatedPointDate >= from && calculatedPointDate <= to) {
                    timestamps[lastPointIndex] = calculatedPointDate;

                    //Check if the point refers to an index
                    if (p.hasVIndex()) {
                        value = pList.get(p.getVIndex()).getV();
                    } else {
                        value = p.getV();
                    }
                    values[lastPointIndex] = value;
                    lastPointIndex++;
                }
            }
            builder.points(new LongList(timestamps, lastPointIndex), new DoubleList(values, lastPointIndex));

        } catch (IOException e) {
            LOGGER.info("Could not decode protocol buffers points");
        }

    }

    private static long calculatePoint(MetricProtocolBuffers.Point p, long lastOffset) {
        //Normal delta
        if (p.hasTint() || p.hasTlong()) {
            lastOffset = p.getTint() + p.getTlong();
        }
        //Base point delta
        if (p.hasTintBP() || p.hasTlongBP()) {
            lastOffset = p.getTintBP() + p.getTlongBP();
        }
        return lastOffset;
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points
     * @return the serialized points as byte[]
     */
    public static byte[] to(Iterator<Point> metricDataPoints) {
        return to(metricDataPoints, 0);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points
     * @param almostEquals     - the aberration threshold for the deltas
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<Point> metricDataPoints, final int almostEquals) {

        long previousDate = 0;
        long previousOffset = 0;
        long previousDrift = 0;

        int timesSinceLastOffset = 0;
        long lastStoredDate = 0;
        long lastStoredOffset = 0;

        long startDate = 0;

        double currentValue;

        Map<Double, Integer> valueIndex = new HashMap<>();

        int index = 0;

        MetricProtocolBuffers.Point.Builder point = MetricProtocolBuffers.Point.newBuilder();
        MetricProtocolBuffers.Points.Builder points = MetricProtocolBuffers.Points.newBuilder();

        long offset = 0;

        while (metricDataPoints.hasNext()) {

            Point p = metricDataPoints.next();
            boolean lastPoint = !metricDataPoints.hasNext();
            point.clear();

            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }

            long currentTimestamp = p.getTimestamp();

            currentValue = p.getValue();

            //build value index
            if (valueIndex.containsKey(currentValue)) {
                point.setVIndex(valueIndex.get(currentValue));
            } else {
                valueIndex.put(currentValue, index);
                point.setV(currentValue);
            }

            if (previousDate == 0) {
                // set lastStoredDate to the value of the first timestamp
                lastStoredDate = currentTimestamp;
                startDate = currentTimestamp;
            } else {
                offset = currentTimestamp - previousDate;
            }

            //Semantic Compression
            if (almostEquals == -1) {
                points.addP(point.build());
            } else {

                //we always store the first an the last point as supporting points
                //Date-Delta-Compaction is within a defined start and end
                if (lastPoint) {

                    long calcPoint = calcPoint(startDate, points.getPList(), almostEquals);
                    //Calc offset
                    long offsetToEnd = currentTimestamp - calcPoint;

                    //everything okay
                    if (offsetToEnd >= 0) {
                        if (safeLongToUInt(offsetToEnd)) {
                            points.addP(point.setTint((int) offsetToEnd).build());
                        } else {
                            points.addP(point.setTlong(offsetToEnd).build());
                        }

                    } else {
                        //break the offset down on all points
                        long avgPerDelta = (long) Math.ceil((double) offsetToEnd * -1 + almostEquals / (double) (points.getPCount() - 1));

                        for (int i = 1; i < points.getPCount(); i++) {
                            MetricProtocolBuffers.Point mod = points.getP(i);
                            long t = getT(mod);

                            //check if can correct the deltas
                            if (offsetToEnd < 0) {
                                long newOffset;

                                if (offsetToEnd + avgPerDelta > 0) {
                                    avgPerDelta = offsetToEnd * -1;
                                }

                                //if we have a t value
                                if (t > avgPerDelta) {
                                    newOffset = t - avgPerDelta;
                                    MetricProtocolBuffers.Point.Builder modPoint = mod.toBuilder();
                                    setT(modPoint, newOffset);
                                    mod = modPoint.build();
                                    offsetToEnd += avgPerDelta;
                                }

                            }
                            points.setP(i, mod);
                        }


                        //Done
                        long arragendPoint = calcPoint(startDate, points.getPList(), almostEquals);

                        long storedOffsetToEnd = currentTimestamp - arragendPoint;
                        if (storedOffsetToEnd < 0) {
                            LOGGER.warn("Stored offset is negative. Setting to 0. But that is an error.");
                            storedOffsetToEnd = 0;
                        }
                        if (safeLongToUInt(storedOffsetToEnd)) {
                            points.addP(point.setTintBP((int) storedOffsetToEnd).build());
                        } else {
                            points.addP(point.setTlongBP(storedOffsetToEnd).build());
                        }
                    }

                } else {


                    boolean isAlmostEquals = almostEquals(previousOffset, offset, almostEquals);
                    long drift = 0;
                    if (isAlmostEquals) {
                        drift = drift(currentTimestamp, lastStoredDate, timesSinceLastOffset, lastStoredOffset);
                    }

                    if (isAlmostEquals && noDrift(drift, almostEquals, timesSinceLastOffset) && drift >= 0) {
                        points.addP(point.build());
                        timesSinceLastOffset += 1;
                    } else {
                        long timeStamp = offset;

                        //If the previous offset was not stored, correct the following offset using the calculated drift
                        if (timesSinceLastOffset > 0 && offset > previousDrift) {
                            timeStamp = offset - previousDrift;

                            if (safeLongToUInt(timeStamp)) {
                                point.setTintBP((int) timeStamp);
                            } else {
                                point.setTlongBP(timeStamp);
                            }

                        } else {
                            if (safeLongToUInt(timeStamp)) {
                                point.setTint((int) timeStamp);
                            } else {
                                point.setTlong(timeStamp);
                            }
                        }

                        //Store offset
                        points.addP(point.build());
                        //reset the offset counter
                        timesSinceLastOffset = 0;
                        lastStoredDate = p.getTimestamp();
                        lastStoredOffset = timeStamp;

                    }
                    //set current as former previous date
                    previousDrift = drift;
                    previousOffset = offset;
                    previousDate = currentTimestamp;
                }
            }
            index++;
        }
        //set the ddc value
        points.setDdc(almostEquals);
        return points.build().toByteArray();
    }

    /**
     * Sets the new t for the point. Checks which t was set.
     *
     * @param builder   the point builder
     * @param newOffset the new offset
     */
    private static void setT(MetricProtocolBuffers.Point.Builder builder, long newOffset) {
        if (safeLongToUInt(newOffset)) {
            if (builder.hasTintBP()) {
                builder.setTintBP((int) newOffset);
            }
            if (builder.hasTint()) {
                builder.setTint((int) newOffset);
            }
        } else {
            if (builder.hasTlongBP()) {
                builder.setTlongBP(newOffset);
            }
            if (builder.hasTlong()) {
                builder.setTlong(newOffset);
            }
        }

    }

    /**
     * @param point the current point
     * @return the value of t
     */
    private static long getT(MetricProtocolBuffers.Point point) {
        //only one is set, others are zero
        return point.getTlongBP() + point.getTlong() + point.getTint() + point.getTintBP();
    }

    private static boolean safeLongToUInt(long l) {
        return !(l < 0 || l > Integer.MAX_VALUE);
    }

    private static long calcPoint(long startDate, List<MetricProtocolBuffers.Point> pList, long almostEquals) {

        long lastOffset = almostEquals;
        long calculatedPointDate = startDate;

        for (int i = 1; i < pList.size(); i++) {
            MetricProtocolBuffers.Point p = pList.get(i);
            lastOffset = calculatePoint(p, lastOffset);
            calculatedPointDate += lastOffset;
        }
        return calculatedPointDate;
    }


    private static boolean noDrift(long drift, long almostEquals, long timeSinceLastStoredOffset) {
        return timeSinceLastStoredOffset == 0 || drift == 0 || drift < (almostEquals / 2);
    }

    /**
     * Calculates the drift of the time stamp compaction
     *
     * @param timestamp            the current time stamp
     * @param lastStoredDate       the last stored time stamp
     * @param timesSinceLastOffset times since the last time stamp was stored
     * @param lastStoredOffset     the last stored offset
     */


    private static long drift(long timestamp, long lastStoredDate, int timesSinceLastOffset, long lastStoredOffset) {
        long calculatedMaxOffset = lastStoredOffset * (timesSinceLastOffset + 1);
        return lastStoredDate + calculatedMaxOffset - timestamp;
    }

    /**
     * Check if two deltas are almost equals.
     * <p>
     * abs(offset - previousOffset) <= aberration
     * </p>
     *
     * @param previousOffset the previous offset
     * @param offset         the current offset
     * @param almostEquals   the threshold for equality
     * @return true if set offsets are equals using the threshold
     */
    private static boolean almostEquals(long previousOffset, long offset, long almostEquals) {
        //check the deltas
        double diff = Math.abs(offset - previousOffset);
        return (diff <= almostEquals);
    }

}

