package de.qaware.chronix.timeseries

/**
 * Created by c.hillmann on 07.04.2016.
 */
class MetricTimeSeriesPercentileTest {
    def "test percentile"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("P");
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.percentile(0.5)
        then:
        result == 50.0
    }

    def "test for empty time series"() {
        when:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Empty");

        def result = timeSeries.build().percentile(0.5)
        then:
        result == Double.NaN
    }
}
