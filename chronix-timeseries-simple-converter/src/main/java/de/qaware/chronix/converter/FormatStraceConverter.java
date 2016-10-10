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

import de.qaware.chronix.converter.common.Compression;
import de.qaware.chronix.converter.common.MetricTSSchema;
import de.qaware.chronix.converter.serializer.ProtoBufFormatStraceSerializer;
import de.qaware.chronix.timeseries.StraceTimeSeries;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * The kassiopeia time series converter for the simple time series class
 *
 * @author f.lautenschlager
 */
public class FormatStraceConverter implements TimeSeriesConverter<StraceTimeSeries> {

    public static final String DATA_AS_JSON_FIELD = "dataAsJson";

    private static final Logger LOGGER = LoggerFactory.getLogger(FormatStraceConverter.class);

    @Override
    public StraceTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        LOGGER.debug("Converting {} to StraceTimeseries starting at {} and ending at {}", binaryTimeSeries, queryStart, queryEnd);
        //get the metric
        String metric = binaryTimeSeries.get(MetricTSSchema.METRIC).toString();

        //Third build a minimal time series
        StraceTimeSeries.Builder builder = new StraceTimeSeries.Builder(metric);

        //add all user defined attributes
        binaryTimeSeries.getFields().forEach((field, value) -> {
            if (MetricTSSchema.isUserDefined(field)) {
                builder.attribute(field, value);
            }
        });


        //Default serialization is protocol buffers.
        if (binaryTimeSeries.getPoints().length > 0) {
            fromProtocolBuffers(binaryTimeSeries, queryStart, queryEnd, builder);

        } else if (binaryTimeSeries.getFields().containsKey(DATA_AS_JSON_FIELD)) {
            //do it from json
            fromJson(binaryTimeSeries, queryStart, queryEnd, builder);
        } else {
            //we have no data
            //set the start and end
            builder.start(binaryTimeSeries.getStart());
            builder.end(binaryTimeSeries.getEnd());
        }

        return builder.build();
    }

    private void fromProtocolBuffers(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd, StraceTimeSeries.Builder builder) {
        final InputStream decompressed = Compression.decompressToStream(binaryTimeSeries.getPoints());
        ProtoBufFormatStraceSerializer.from(decompressed, binaryTimeSeries.getStart(), binaryTimeSeries.getEnd(), queryStart, queryEnd, builder);
        IOUtils.closeQuietly(decompressed);
    }

    private void fromJson(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd, StraceTimeSeries.Builder builder) {
        String jsonString = binaryTimeSeries.get(DATA_AS_JSON_FIELD).toString();
        //Second deserialize
        // JsonKassiopeiaSimpleSerializer serializer = new JsonKassiopeiaSimpleSerializer();
        //serializer.fromJson(jsonString.getBytes(Charset.forName(JsonKassiopeiaSimpleSerializer.UTF_8)), queryStart, queryEnd, builder);
    }

    @Override
    public BinaryTimeSeries to(StraceTimeSeries timeSeries) {
        LOGGER.debug("Converting {} to BinaryTimeSeries", timeSeries);
        BinaryTimeSeries.Builder builder = new BinaryTimeSeries.Builder();

        //serialize
        byte[] serializedPoints = ProtoBufFormatStraceSerializer.to(timeSeries.points().iterator());
        byte[] compressedPoints = Compression.compress(serializedPoints);

        //Add the minimum required fields
        builder.start(timeSeries.getStart())
                .end(timeSeries.getEnd())
                .data(compressedPoints);

        //Currently we only have a metric
        builder.field(MetricTSSchema.METRIC, timeSeries.getMetric());

        //Add a list of user defined attributes
        timeSeries.attributes().forEach(builder::field);

        return builder.build();
    }
}
