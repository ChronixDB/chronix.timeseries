package de.qaware.chronix.timeseries

/**
 * Created by c.hillmann on 07.04.2016.
 */
class MetricTimeSeriesStdDeviationTest {

    def "test execute"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Stddev");
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()


        when:
        def result = ts.stdDeviation()
        then:
        result == 3001.381363790528
    }

    def "test for empty time series"() {
        when:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Empty");

        def result = timeSeries.build().stdDeviation()
        then:
        result == Double.NaN
    }

}
