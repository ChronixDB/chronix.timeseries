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
package de.qaware.chronix.converter;

import de.qaware.chronix.timeseries.MetricTimeSeries;
import de.qaware.chronix.timeseries.TimeSeries;
import de.qaware.chronix.timeseries.dts.Pair;
import de.qaware.chronix.timeseries.dts.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Generic time series converter to convert our time series into a binary storage time series and back
 *
 * @author f.lautenschlager
 */
public class AdvancedTimeSeriesConverter implements TimeSeriesConverter<TimeSeries<Long, Double>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedTimeSeriesConverter.class);

    @Override
    public TimeSeries<Long, Double> from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {

        //This is a hack
        MetricTimeSeries metricTimeSeries = new MetricTimeSeriesConverter().from(binaryTimeSeries, queryStart, queryEnd);
        TimeSeries<Long, Double> timeSeries = new TimeSeries<>(map(metricTimeSeries.points()));
        metricTimeSeries.getAttributesReference().forEach(timeSeries::addAttribute);

        return timeSeries;
    }

    private Iterator<Pair<Long, Double>> map(Stream<Point> points) {
        return points.map(point -> Pair.pairOf(point.getTimestamp(), point.getValue())).iterator();
    }


    @Override
    public BinaryTimeSeries to(TimeSeries<Long, Double> timeSeries) {

        //-oo is represented through the first element that is null, hence if the size is one the time series is empty
        if (timeSeries.size() == 1) {
            LOGGER.info("Empty time series detected. {}", timeSeries);
            //Create a builder with the minimal required fields
            BinaryTimeSeries.Builder builder = new BinaryTimeSeries.Builder()
                    .data(new byte[]{})
                    .start(0)
                    .end(0);
            return builder.build();
        } else {
            return new MetricTimeSeriesConverter().to(map(timeSeries));
        }
    }

    private MetricTimeSeries map(TimeSeries<Long, Double> timeSeries) {
        MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(timeSeries.getAttribute("metric").toString());


        //add points
        Iterator<Pair<Long, Double>> it = timeSeries.iterator();

        //ignore the first element
        if (it.hasNext()) {
            it.next();
        }

        while (it.hasNext()) {
            Pair<Long, Double> pair = it.next();
            builder.point(pair.getFirst(), pair.getSecond());
        }

         //add attributes
        timeSeries.getAttributes().forEachRemaining(attribute -> builder.attribute(attribute.getKey(), attribute.getValue()));

        return builder.build();
    }
}
