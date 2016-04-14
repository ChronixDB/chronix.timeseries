package de.qaware.chronix.converter;

import de.qaware.chronix.timeseries.StraceTimeSeries;

/**
 * Created by max on 14.04.16.
 */
public class StraceTimeSeriesConverter implements TimeSeriesConverter<StraceTimeSeries> {

    @Override
    public StraceTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        return null;
    }

    @Override
    public BinaryTimeSeries to(StraceTimeSeries document) {
        return null;
    }
}
