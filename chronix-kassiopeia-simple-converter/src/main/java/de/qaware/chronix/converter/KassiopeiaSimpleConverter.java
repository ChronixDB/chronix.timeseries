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
package de.qaware.chronix.converter;

import de.qaware.chronix.schema.MetricTSSchema;
import de.qaware.chronix.serializer.JsonKassiopeiaSimpleSerializer;
import de.qaware.chronix.timeseries.MetricTimeSeries;

import java.util.List;

/**
 * The kassiopeia time series converter for the simple time series class
 *
 * @author f.lautenschlager
 */
public class KassiopeiaSimpleConverter implements TimeSeriesConverter<MetricTimeSeries> {

    @Override
    public MetricTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {

        //get the metric
        String metric = binaryTimeSeries.get(MetricTSSchema.METRIC).toString();

        //Third build a minimal time series
        MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(metric);

        if (binaryTimeSeries.getPoints().length > 0) {
            //Decompress if we have a data field
            byte[] decompressedJson = Compression.decompress(binaryTimeSeries.getPoints());

            //Second deserialize
            JsonKassiopeiaSimpleSerializer serializer = new JsonKassiopeiaSimpleSerializer();
            List[] timestampValues = serializer.fromJson(decompressedJson, queryStart, queryEnd);

            builder.data((List<Long>) timestampValues[0], (List<Double>) timestampValues[1]);

        } else {
            //we have a aggregation result
            double value = Double.valueOf(binaryTimeSeries.get("value").toString());

            long meanDate = meanDate(binaryTimeSeries);
            builder.point(meanDate, value);
        }


        //add all user defined attributes
        binaryTimeSeries.getFields().forEach((field, value) -> {
            if (MetricTSSchema.isUserDefined(field)) {
                builder.attribute(field, value);
            }
        });

        return builder.build();
    }

    private long meanDate(BinaryTimeSeries binaryTimeSeries) {
        long start = binaryTimeSeries.getStart();
        long end = binaryTimeSeries.getStart();

        return start + ((end - start) / 2);
    }


    @Override
    public BinaryTimeSeries to(MetricTimeSeries timeSeries) {
        BinaryTimeSeries.Builder builder = new BinaryTimeSeries.Builder();

        JsonKassiopeiaSimpleSerializer serializer = new JsonKassiopeiaSimpleSerializer();
        //serialize
        byte[] json = serializer.toJson(timeSeries.getTimestamps(), timeSeries.getValues());
        byte[] compressedJson = Compression.compress(json);

        //Add the minimum required fields
        builder.start(timeSeries.getStart())
                .end(timeSeries.getEnd())
                .data(compressedJson);

        //Currently we only have a metric
        builder.field(MetricTSSchema.METRIC, timeSeries.getMetric());

        //Add a list of user defined attributes
        timeSeries.attributes().forEach(builder::field);

        return builder.build();
    }
}
