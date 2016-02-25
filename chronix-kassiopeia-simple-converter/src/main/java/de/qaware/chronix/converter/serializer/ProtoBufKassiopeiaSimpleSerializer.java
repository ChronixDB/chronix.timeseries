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
import de.qaware.chronix.converter.serializer.gen.SimpleProtocolBuffers;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import de.qaware.chronix.timeseries.dt.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

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
     * @param from            including points from
     * @param to              including points to
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, MetricTimeSeries.Builder builder) {
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
            InputStream points = Compression.decompressToStream(compressedBytes);
            SimpleProtocolBuffers.Points protocolBufferPoints = SimpleProtocolBuffers.Points.parseFrom(points);

            long lastOffset = ALMOST_EQUALS_OFFSET_MS;
            long calculatedPointDate = timeSeriesStart;

            List<SimpleProtocolBuffers.Point> pList = protocolBufferPoints.getPList();

            for (int i = 0; i < pList.size(); i++) {
                SimpleProtocolBuffers.Point p = pList.get(i);

                if (i > 0) {
                    long offset = p.getT();
                    if (offset != 0) {
                        lastOffset = offset;
                    }
                    calculatedPointDate += lastOffset;
                }

                //only add the point if it is within the date
                if (calculatedPointDate >= from && calculatedPointDate <= to) {
                    builder.point(calculatedPointDate, p.getV());
                }
            }

        } catch (IOException e) {
            LOGGER.info("Could not decode protocol buffers points");
        }

    }

    /**
     * Converts the given iterator of our point class to the protocol buffer format
     *
     * @param metricDataPoints - the list with points
     * @return a protocol buffer points object
     */
    public static byte[] to(Iterator<Point> metricDataPoints) {
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

        return Compression.compress(points.build().toByteArray());
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
}
