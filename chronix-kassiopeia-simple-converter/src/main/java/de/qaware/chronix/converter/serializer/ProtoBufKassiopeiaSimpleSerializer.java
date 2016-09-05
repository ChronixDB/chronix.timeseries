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
package de.qaware.chronix.converter.serializer;


import de.qaware.chronix.converter.common.Compression;
import de.qaware.chronix.converter.common.DoubleList;
import de.qaware.chronix.converter.common.LongList;
import de.qaware.chronix.converter.serializer.gen.SimpleProtocolBuffers;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import de.qaware.chronix.timeseries.Point;
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
public final class ProtoBufKassiopeiaSimpleSerializer {

    /**
     * Name of the system property to set the equals offset between the dates.
     */
    public static final String DATE_EQUALS_OFFSET_MS = "DATE_EQUALS_OFFSET_MS";

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufKassiopeiaSimpleSerializer.class);
    private static final long ALMOST_EQUALS_OFFSET_MS = Long.parseLong(System.getProperty(DATE_EQUALS_OFFSET_MS, "10"));

    /**
     * Private constructor
     */
    private ProtoBufKassiopeiaSimpleSerializer() {
        //utility class
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param compressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart the start of the time series
     * @param timeSeriesEnd   the end of the time series
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, MetricTimeSeries.Builder builder) {
        from(compressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, builder);
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param compressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart the start of the time series
     * @param timeSeriesEnd   the end of the time series
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, long almost_equals_ms, MetricTimeSeries.Builder builder) {
        from(compressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, almost_equals_ms, builder);
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param compressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart the start of the time series
     * @param timeSeriesEnd   the end of the time series
     * @param from            including points from
     * @param to              including points to
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, MetricTimeSeries.Builder builder) {
        from(compressedBytes, timeSeriesStart, timeSeriesEnd, from, to, ALMOST_EQUALS_OFFSET_MS, builder);
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param compressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart the start of the time series
     * @param timeSeriesEnd   the end of the time series
     * @param from            including points from
     * @param to              including points to
     * @param almostEqualsMs  the aberration for the deltas
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, long almostEqualsMs, MetricTimeSeries.Builder builder) {
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
            InputStream decompressedPointStream = Compression.decompressToStream(compressedBytes);
            SimpleProtocolBuffers.Points protocolBufferPoints = SimpleProtocolBuffers.Points.parseFrom(decompressedPointStream);

            List<SimpleProtocolBuffers.Point> pList = protocolBufferPoints.getPList();

            int size = pList.size();
            SimpleProtocolBuffers.Point[] points = pList.toArray(new SimpleProtocolBuffers.Point[0]);

            long[] timestamps = new long[pList.size()];
            double[] values = new double[pList.size()];

            long lastOffset = almostEqualsMs;
            long calculatedPointDate = timeSeriesStart;
            int lastPointIndex = 0;

            double value;

            for (int i = 0; i < size; i++) {
                SimpleProtocolBuffers.Point p = points[i];

                //Decode the time
                if (i > 0) {
                    lastOffset = calculatePoint(p, lastOffset);
                    calculatedPointDate += lastOffset;
                    //rest the offset
                    if (almostEqualsMs > 0 && (p.hasTintBP() || p.hasTlongBP())) {
                        lastOffset = almostEqualsMs;
                    }
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

    private static long calculatePoint(SimpleProtocolBuffers.Point p, long lastOffset) {
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
     */
    public static byte[] to(Iterator<Point> metricDataPoints) {
        return to(metricDataPoints, ALMOST_EQUALS_OFFSET_MS);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points
     * @param almostEquals     - the aberration threshold for the deltas
     * @return a protocol buffer points object
     */
    public static byte[] to(final Iterator<Point> metricDataPoints, final long almostEquals) {

        long previousDate = 0;
        long previousOffset = 0;
        long previousDrift = 0;

        int timesSinceLastOffset = 0;
        long lastStoredDate = 0;

        long startDate = 0;

        double currentValue;

        Map<Double, Integer> valueIndex = new HashMap<>();

        int index = 0;

        SimpleProtocolBuffers.Point.Builder point = SimpleProtocolBuffers.Point.newBuilder();
        SimpleProtocolBuffers.Points.Builder points = SimpleProtocolBuffers.Points.newBuilder();

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


            long offset = 0;
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
                        if (safeLongToInt(offsetToEnd)) {
                            points.addP(point.setTint((int) offsetToEnd).build());
                        } else {
                            points.addP(point.setTlong(offsetToEnd).build());
                        }

                    } else {
                        //break the offset down on all points
                        long avgPerDelta = (long) Math.ceil((double) offsetToEnd * -1 + almostEquals / (double) (points.getPCount() - 1));

                        for (int i = 1; i < points.getPCount(); i++) {
                            SimpleProtocolBuffers.Point mod = points.getP(i);
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
                                    setT(mod.toBuilder(), newOffset);
                                }

                                offsetToEnd += avgPerDelta;
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
                        if (safeLongToInt(storedOffsetToEnd)) {
                            points.addP(point.setTintBP(storedOffsetToEnd).build());
                        } else {
                            points.addP(point.setTlongBP(storedOffsetToEnd).build());
                        }
                    }

                } else {

                    long drift = drift(currentTimestamp, lastStoredDate, timesSinceLastOffset, almostEquals);

                    if (almostEquals(previousOffset, offset, almostEquals) && noDrift(drift, almostEquals)) {
                        points.addP(point.build());
                        timesSinceLastOffset += 1;
                    } else {
                        long timeStamp = offset;

                        //If the previous offset was not stored, correct the following offset using the calculated drift
                        if (timesSinceLastOffset > 1 && offset > previousDrift) {
                            timeStamp = offset - previousDrift;

                            if (safeLongToInt(timeStamp)) {
                                point.setTintBP((int) timeStamp);
                            } else {
                                point.setTlongBP(timeStamp);
                            }

                        } else {
                            if (safeLongToInt(timeStamp)) {
                                point.setTint((int) timeStamp);
                            } else {
                                point.setTlong(timeStamp);
                            }
                        }

                        //Store offset
                        points.addP(point.build());
                        //reset the offset counter
                        timesSinceLastOffset = 1;
                        lastStoredDate = p.getTimestamp();
                    }
                    //set current as former previous date
                    previousDrift = drift;
                    previousOffset = offset;
                    previousDate = currentTimestamp;
                }
            }
            index++;
        }
        //todo: optimize
        return Compression.compress(points.build().toByteArray());
    }

    /**
     * Sets the new t for the point. Checks which t was set.
     *
     * @param builder   the point builder
     * @param newOffset the new offset
     */
    private static void setT(SimpleProtocolBuffers.Point.Builder builder, long newOffset) {
        if (safeLongToInt(newOffset)) {
            if (builder.hasTintBP()) {
                builder.setTintBP((int) newOffset);
            }
            if (builder.hasTint()) {
                builder.setTint((int) newOffset);
            }
        } else {
            if (builder.hasTintBP()) {
                builder.setTlongBP(newOffset);
            }
            if (builder.hasTint()) {
                builder.setTlong(newOffset);
            }
        }

    }

    /**
     * @param point the current point
     * @return the value of t
     */
    private static long getT(SimpleProtocolBuffers.Point point) {
        //only one is set, others are zero
        return point.getTlongBP() + point.getTlong() + point.getTint() + point.getTintBP();
    }

    private static boolean safeLongToInt(long l) {
        return !(l < Integer.MIN_VALUE || l > Integer.MAX_VALUE);
    }


    private static long calcPoint(long startDate, List<SimpleProtocolBuffers.Point> pList, long almostEquals) {

        long lastOffset = almostEquals;
        long calculatedPointDate = startDate;

        for (int i = 1; i < pList.size(); i++) {
            SimpleProtocolBuffers.Point p = pList.get(i);
            lastOffset = calculatePoint(p, lastOffset);
            calculatedPointDate += lastOffset;

            if (almostEquals > 0 && (p.hasTintBP() || p.hasTlongBP())) {
                lastOffset = almostEquals;
            }
        }
        return calculatedPointDate;
    }


    private static boolean noDrift(long drift, long almostEquals) {
        return drift == 0 || drift < (almostEquals / 2);
    }

    /**
     * Calculates the drift of the time stamp compaction
     *
     * @param timestamp            the current time stamp
     * @param lastStoredDate       the last stored time stamp
     * @param timesSinceLastOffset times since the last time stamp was stored
     * @param almostEquals         the aberration threshold
     * @return 0 if the drift is negative, or the drift
     */
    private static long drift(long timestamp, long lastStoredDate, int timesSinceLastOffset, long almostEquals) {
        long calculatedMaxOffset = almostEquals * timesSinceLastOffset;
        long drift = lastStoredDate + calculatedMaxOffset - timestamp;

        if (drift > 0) {
            return drift;
        } else {
            return 0;
        }
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

