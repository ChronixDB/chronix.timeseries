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


import de.qaware.chronix.converter.common.LongList;
import de.qaware.chronix.converter.serializer.gen.LsofProtocolBuffers;
import de.qaware.chronix.timeseries.Lsof;
import de.qaware.chronix.timeseries.LsofPoint;
import de.qaware.chronix.timeseries.LsofTimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Class to easily convert the protocol buffer into Point<Long,Double>
 *
 * @author f.lautenschlager
 */
public final class ProtoBufFormatLsofSerializer {

    /**
     * Name of the system property to set the equals offset between the dates.
     */
    public static final String DATE_EQUALS_OFFSET_MS = "DATE_EQUALS_OFFSET_MS";

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufFormatLsofSerializer.class);
    private static final long ALMOST_EQUALS_OFFSET_MS = Long.parseLong(System.getProperty(DATE_EQUALS_OFFSET_MS, "10"));

    /**
     * Private constructor
     */
    private ProtoBufFormatLsofSerializer() {
        //utility class
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     * @param builder           the time series builder
     */
    public static void from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, LsofTimeSeries.Builder builder) {
        from(decompressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, builder);
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     * @param builder           the time series builder
     */
    public static void from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long almost_equals_ms, LsofTimeSeries.Builder builder) {
        from(decompressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, almost_equals_ms, builder);
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
    public static void from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, LsofTimeSeries.Builder builder) {
        from(decompressedBytes, timeSeriesStart, timeSeriesEnd, from, to, ALMOST_EQUALS_OFFSET_MS, builder);
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     * @param from              including points from
     * @param to                including points to
     * @param almostEqualsMs    the aberration for the deltas
     * @param builder           the time series builder
     */
    public static void from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, long almostEqualsMs, LsofTimeSeries.Builder builder) {
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
            LsofProtocolBuffers.Lsof lsofProtocoLBuffers = LsofProtocolBuffers.Lsof.parseFrom(decompressedBytes);

            List<LsofProtocolBuffers.LsofPoints> pList = lsofProtocoLBuffers.getPList();

            int size = pList.size();

            long[] timestamps = new long[pList.size()];
            List<List<Lsof>> values = new ArrayList<>();

            long lastOffset = almostEqualsMs;
            long calculatedPointDate = timeSeriesStart;
            int lastPointIndex = 0;


            for (int i = 0; i < size; i++) {
                LsofProtocolBuffers.LsofPoints p = pList.get(i);

                //Decode the time
                if (i > 0) {
                    lastOffset = calculatePoint(p, lastOffset);
                    calculatedPointDate += lastOffset;
                }

                //only add the point if it is within the date
                if (calculatedPointDate >= from && calculatedPointDate <= to) {
                    timestamps[lastPointIndex] = calculatedPointDate;
                    values.add(convert(p.getPList()));
                    lastPointIndex++;
                }
            }
            builder.points(new LongList(timestamps, lastPointIndex), values);

        } catch (IOException e) {
            LOGGER.info("Could not decode protocol buffers points");
        }

    }

    private static List<Lsof> convert(List<LsofProtocolBuffers.LsofPoint> pList) {
        List<Lsof> converted = new ArrayList<>(pList.size());

        for (LsofProtocolBuffers.LsofPoint lsof : pList) {
            converted.add(new Lsof.Builder()
                    .command(lsof.getCommand())
                    .pid(lsof.getPid())
                    .user(lsof.getUser())
                    .fd(lsof.getUser())
                    .device(lsof.getDevice())
                    .size(lsof.getSize())
                    .node(lsof.getNode())
                    .node(lsof.getName())
                    .build());
        }
        return converted;
    }

    private static long calculatePoint(LsofProtocolBuffers.LsofPoints p, long lastOffset) {
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
     * @param lsofPoints - the list with points
     */
    public static byte[] to(Iterator<LsofPoint> lsofPoints) {
        return to(lsofPoints, ALMOST_EQUALS_OFFSET_MS);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param lsofPointIterator - the list with points
     * @param almostEquals      - the aberration threshold for the deltas
     * @return the serialized points
     */
    public static byte[] to(final Iterator<LsofPoint> lsofPointIterator, final long almostEquals) {

        long previousDate = 0;
        long previousOffset = 0;
        long previousDrift = 0;

        int timesSinceLastOffset = 0;
        long lastStoredDate = 0;
        long lastStoredOffset = 0;

        long startDate = 0;

        Set<Lsof> currentValue;

        LsofProtocolBuffers.LsofPoints.Builder points = LsofProtocolBuffers.LsofPoints.newBuilder();
        LsofProtocolBuffers.Lsof.Builder lsof = LsofProtocolBuffers.Lsof.newBuilder();

        long offset = 0;

        while (lsofPointIterator.hasNext()) {

            LsofPoint p = lsofPointIterator.next();
            boolean lastPoint = !lsofPointIterator.hasNext();
            points.clear();

            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }

            points.addAllP(convertToProto(p.getValue()));


            long currentTimestamp = p.getTimestamp();
            if (previousDate == 0) {
                // set lastStoredDate to the value of the first timestamp
                lastStoredDate = currentTimestamp;
                startDate = currentTimestamp;
            } else {
                offset = currentTimestamp - previousDate;
            }

            //Semantic Compression
            if (almostEquals == -1) {
                lsof.addP(points.build());
            } else {

                //we always store the first an the last point as supporting points
                //Date-Delta-Compaction is within a defined start and end
                if (lastPoint) {

                    long calcPoint = calcPoint(startDate, lsof.getPList(), almostEquals);
                    //Calc offset
                    long offsetToEnd = currentTimestamp - calcPoint;

                    //everything okay
                    if (offsetToEnd >= 0) {
                        if (safeLongToUInt(offsetToEnd)) {
                            lsof.addP(points.setTint((int) offsetToEnd).build());
                        } else {
                            lsof.addP(points.setTlong(offsetToEnd).build());
                        }

                    } else {
                        //break the offset down on all points
                        long avgPerDelta = (long) Math.ceil((double) offsetToEnd * -1 + almostEquals / (double) (lsof.getPCount() - 1));

                        for (int i = 1; i < lsof.getPCount(); i++) {
                            LsofProtocolBuffers.LsofPoints mod = lsof.getP(i);
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
                                    LsofProtocolBuffers.LsofPoints.Builder modPoint = mod.toBuilder();
                                    setT(modPoint, newOffset);
                                    mod = modPoint.build();
                                    offsetToEnd += avgPerDelta;
                                }

                            }
                            lsof.setP(i, mod);
                        }


                        //Done
                        long arragendPoint = calcPoint(startDate, lsof.getPList(), almostEquals);

                        long storedOffsetToEnd = currentTimestamp - arragendPoint;
                        if (storedOffsetToEnd < 0) {
                            LOGGER.warn("Stored offset is negative. Setting to 0. But that is an error.");
                            storedOffsetToEnd = 0;
                        }
                        if (safeLongToUInt(storedOffsetToEnd)) {
                            lsof.addP(points.setTintBP((int) storedOffsetToEnd).build());
                        } else {
                            lsof.addP(points.setTlongBP(storedOffsetToEnd).build());
                        }
                    }

                } else {


                    boolean isAlmostEquals = almostEquals(previousOffset, offset, almostEquals);
                    long drift = 0;
                    if (isAlmostEquals) {
                        drift = drift(currentTimestamp, lastStoredDate, timesSinceLastOffset, lastStoredOffset);
                    }

                    if (isAlmostEquals && noDrift(drift, almostEquals, timesSinceLastOffset) && drift >= 0) {
                        lsof.addP(points.build());
                        timesSinceLastOffset += 1;
                    } else {
                        long timeStamp = offset;

                        //If the previous offset was not stored, correct the following offset using the calculated drift
                        if (timesSinceLastOffset > 0 && offset > previousDrift) {
                            timeStamp = offset - previousDrift;

                            if (safeLongToUInt(timeStamp)) {
                                points.setTintBP((int) timeStamp);
                            } else {
                                points.setTlongBP(timeStamp);
                            }

                        } else {
                            if (safeLongToUInt(timeStamp)) {
                                points.setTint((int) timeStamp);
                            } else {
                                points.setTlong(timeStamp);
                            }
                        }

                        //Store offset
                        lsof.addP(points.build());
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
        }
        return lsof.build().toByteArray();
    }

    private static List<LsofProtocolBuffers.LsofPoint> convertToProto(List<Lsof> value) {
        List<LsofProtocolBuffers.LsofPoint> converted = new ArrayList<>();
        LsofProtocolBuffers.LsofPoint.Builder point = LsofProtocolBuffers.LsofPoint.newBuilder();

        for (Lsof lsof : value) {
            converted.add(point
                    .setCommand(lsof.getCommand())
                    .setPid(lsof.getPid())
                    .setUser(lsof.getUser())
                    .setFd(lsof.getFd())
                    .setDevice(lsof.getDevice())
                    .setSize(lsof.getSize())
                    .setNode(lsof.getNode())
                    .setName(lsof.getName())
                    .build());
        }

        return converted;
    }

    /**
     * Sets the new t for the point. Checks which t was set.
     *
     * @param builder   the point builder
     * @param newOffset the new offset
     */
    private static void setT(LsofProtocolBuffers.LsofPoints.Builder builder, long newOffset) {
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
    private static long getT(LsofProtocolBuffers.LsofPoints point) {
        //only one is set, others are zero
        return point.getTlongBP() + point.getTlong() + point.getTint() + point.getTintBP();
    }

    private static boolean safeLongToUInt(long l) {
        return !(l < 0 || l > Integer.MAX_VALUE);
    }

    private static long calcPoint(long startDate, List<LsofProtocolBuffers.LsofPoints> pList, long almostEquals) {

        long lastOffset = almostEquals;
        long calculatedPointDate = startDate;

        for (int i = 1; i < pList.size(); i++) {
            LsofProtocolBuffers.LsofPoints p = pList.get(i);
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

