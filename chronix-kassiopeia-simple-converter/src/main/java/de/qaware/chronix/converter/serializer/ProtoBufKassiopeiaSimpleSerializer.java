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


import de.qaware.chronix.converter.serializer.gen.SimpleProtocolBuffers;
import de.qaware.chronix.timeseries.dt.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
     * Returns an iterator to the points from the given byte array encoded data string
     *
     * @param points          - the input stream of points
     * @param timeSeriesStart - the start of the time series
     * @param timeSeriesEnd   - the end of the time series
     * @return an iterator to the points
     */
    public static Iterator<Point> from(final InputStream points, long timeSeriesStart, long timeSeriesEnd) {
        return new PointIterator(points, timeSeriesStart, timeSeriesEnd, -1, -1);
    }


    /**
     * Returns an iterator to the points from the given byte array encoded data string
     *
     * @param points          - the input stream of points
     * @param from            - including points from
     * @param to              - including points to
     * @param timeSeriesStart - the start of the time series  @return an iterator to the points
     * @return an iterator of pairs (timestamp, value)
     */
    public static Iterator<Point> from(final InputStream points, long timeSeriesStart, long timeSeriesEnd, long from, long to) {
        if (from == -1 || to == -1) {
            throw new IllegalArgumentException("FROM or TO have to be >= 0");
        }
        return new PointIterator(points, timeSeriesStart, timeSeriesEnd, from, to);
    }

    /**
     * Converts the given iterator of our point class to the protocol buffer format
     *
     * @param metricDataPoints - the list with points
     * @return a protocol buffer points object
     */
    public static SimpleProtocolBuffers.Points to(Iterator<Point> metricDataPoints) {
        long previousDate = 0;
        long previousOffset = 0;

        int timesSinceLastOffset = 1;
        long lastStoredDate = 0;

        SimpleProtocolBuffers.Point.Builder builder = SimpleProtocolBuffers.Point.newBuilder();
        SimpleProtocolBuffers.Points.Builder points = SimpleProtocolBuffers.Points.newBuilder();


        while (metricDataPoints.hasNext()) {

            Point p = metricDataPoints.next();

            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }

            long offset;
            if (previousDate == 0) {
                offset = 0;
            } else {
                offset = p.getTimestamp() - previousDate;
            }

            if (almostEquals(previousOffset, offset) && noDrift(p.getTimestamp(), lastStoredDate, timesSinceLastOffset)) {
                builder.clearT()
                        .setV(p.getValue());
                points.addP(builder.build());
                timesSinceLastOffset += 1;

            } else {
                builder.setT(offset)
                        .setV(p.getValue())
                        .build();
                points.addP(builder.build());
                //reset the offset counter
                timesSinceLastOffset = 1;
                lastStoredDate = p.getTimestamp();
            }
            //set current as former previous date
            previousOffset = offset;
            previousDate = p.getTimestamp();
        }

        return points.build();
    }

    private static boolean noDrift(long timestamp, long lastStoredDate, int timesSinceLastOffset) {
        long calculatedMaxOffset = ALMOST_EQUALS_OFFSET_MS * timesSinceLastOffset;
        long drift = lastStoredDate + calculatedMaxOffset - timestamp;

        return (drift <= (ALMOST_EQUALS_OFFSET_MS / 2));
    }


    private static boolean almostEquals(long previousOffset, long offset) {
        double diff = Math.abs(offset - previousOffset);
        return (diff <= ALMOST_EQUALS_OFFSET_MS);
    }


    /**
     * Iterator that returns Point<Long,Double> from the decompressed input stream
     */
    private static class PointIterator implements Iterator<Point> {

        private int size;
        private int current = 0;
        private SimpleProtocolBuffers.Points protocolBufferPoints;
        private long lastOffset = 0;
        private long lastDate;
        private long timeSeriesEnd;
        private long timeSeriesStart;

        private long from;
        private long to;

        /**
         * Constructs a point iterator
         *
         * @param pointStream     - the points
         * @param from            - including points from
         * @param to              - including points to
         * @param timeSeriesStart - start of the time series
         */
        public PointIterator(final InputStream pointStream, long timeSeriesStart, long timeSeriesEnd, long from, long to) {
            try {
                protocolBufferPoints = SimpleProtocolBuffers.Points.parseFrom(pointStream);
                size = protocolBufferPoints.getPCount();

                this.timeSeriesStart = timeSeriesStart;
                this.timeSeriesEnd = timeSeriesEnd;
                this.from = from;
                this.to = to;

                this.lastDate = timeSeriesStart;

            } catch (Exception e) {
                throw new IllegalStateException("Could not decode data to protocol buffer points.", e);
            }
        }

        /**
         * return true if the iterator points to more points
         */
        @Override
        public boolean hasNext() {

            //check if the given time range is valid
            if (from != -1 & to != -1) {
                //if to is left of the time series, we have no points to return
                if (to < timeSeriesStart) {
                    return false;
                }
                //if from is greater  to, we have nothing to return
                if (from > to) {
                    return false;
                }
                //if from is right of the time series we have nothing to return
                if (from > timeSeriesEnd) {
                    return false;
                }

                //check if the last date is greater than to
                if (lastDate >= to) {
                    return false;
                }

            }

            //otherwise we check the current index position
            return size > current;
        }

        /**
         * @return the next point
         */
        @Override
        public Point next() {

            if (!hasNext()) {
                throw new NoSuchElementException("No more elements.");
            }

            Point currentPoint = nextPoint();

            while (lastDate < from) {
                currentPoint = nextPoint();
            }

            return currentPoint;
        }

        private Point nextPoint() {
            Point p = convertToPoint(protocolBufferPoints.getP(current), lastDate);
            lastDate = p.getTimestamp();
            current++;
            return p;
        }

        private Point convertToPoint(SimpleProtocolBuffers.Point m, long lastDate) {
            //set the default offset for following points
            if (current == 1) {
                lastOffset = ALMOST_EQUALS_OFFSET_MS;
            }

            long offset = m.getT();
            if (offset != 0) {
                lastOffset = offset;
            }
            return new Point(current, lastDate + lastOffset, m.getV());
        }

        @Override
        public void remove() {
            //does not make sense
        }
    }
}
