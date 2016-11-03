[![Build Status](https://travis-ci.org/ChronixDB/chronix.timeseries.svg)](https://travis-ci.org/ChronixDB/chronix.timeseries)
[![Coverage Status](https://coveralls.io/repos/github/ChronixDB/chronix.timeseries/badge.svg?branch=master)](https://coveralls.io/github/ChronixDB/chronix.timeseries?branch=master)
[![Dependency Status](https://dependencyci.com/github/ChronixDB/chronix.timeseries/badge)](https://dependencyci.com/github/ChronixDB/chronix.timeseries)
[![Sputnik](https://sputnik.ci/conf/badge)](https://sputnik.ci/app#/builds/ChronixDB/chronix.timeseries)
[![Apache License 2](http://img.shields.io/badge/license-ASF2-blue.svg)](https://github.com/ChronixDB/chronix.timeseries/blob/master/LICENSE)
[ ![Download](https://api.bintray.com/packages/chronix/maven/chronix-timeseries/images/download.svg) ](https://bintray.com/chronix/maven/chronix-timeseries/_latestVersion)

# Chronix Time Series
Chronix time series is a library that provides classes and functions to work with time related data and can easily plugged into Chronix.
It is split up into different time series classes with its belonging converters.
It has no dependencies to Chronix and can be used in any project without putting other Chronix libraries on the classpath.

## Chronix generic time series
That's how the Chronix generic time series looks (A detailed description could be found here:
[Chronix-TimeSeries](https://github.com/ChronixDB/chronix.timeseries/tree/master/chronix-timeseries)):
```java
	ArrayList<Pair<Integer, Integer>> aux = new ArrayList<>();
	aux.add(pairOf(0, 7));
	aux.add(pairOf(10, 70));
	GenericTimeSeries<Integer, Integer> tv = new GenericTimeSeries<>(aux);
	sf.apply(-10000);          // returns null
	sf.apply(0);               // returns 7
	sf.apply(10);              // returns 70
	sf.apply(1000);            // returns 70

	aux.clear();
	aux.add(pairOf(null, 3));
	aux.add(pairOf(5, 30));
	GenericTimeSeries<Integer, Integer> tw = new GenericTimeSeries<>(aux);

	GenericTimeSeries<Integer, Boolean> tr = merge(tv, tw, (x, y) -> x < y);
	tr.apply(-10000);          // returns True
	tr.apply(0);               // returns False
	tr.apply(5);               // returns True
	tr.apply(10);              // returns False
```

### Chronix generic time series converter
To use generic time series with Chronix-Server one need a time series converter that depends on the Chronix-API package. 
Both packages are on Bintray.
**Note:** Currently only doubles as values are supported. More to come within the next releases. Currently the GenericTimeSeriesConverter uses the MetricTimeSeriesConverter.

This is a code snipped that shows how to integrate generic time series and Chronix-Server:
```Java
//Define a group by function for the time series records
Function<GenericTimeSeries<Long, Double>, String> groupBy = ts -> ts.getAttribute("metric") 
                                                           + "-"
                                                           + ts.getAttribute("host");

//Define a reduce function for the grouped time series records. We use the average.
BinaryOperator<GenericTimeSeries<Long, Double>> reduce = (ts1, ts2) -> 
                                                   merge(ts1, ts2, (y1, y2) -> (y1 + y2) / 2);

//Instantiate a Chronix Client
ChronixClient<GenericTimeSeries<Long, Double>, SolrClient, SolrQuery> chronix = new ChronixClient<>
                    (new GenericTimeSeriesConverter(), new ChronixSolrStorage<>(200, groupBy, reduce));

//We want all time series that metric matches *load*.
SolrQuery query = new SolrQuery("metric:*Load*");

//The result is a Java Stream. We simply collect the result into a list.
List<GenericTimeSeries<Long, Double>> result = chronix.stream(solr, query).collect(Collectors.toList());
```

## Chronix metric time series
The metric time series is a time series with pairs of long and double. That is a typical time series implementation. 
**Note:** The Chronix-Server uses for server-side functions the metric time series.
That's how the metric time series looks:
```java
//Build a metric time series with three points
MetricTimeSeries series = new MetricTimeSeries.Builder("memory\\usage")
      .attribute("host", "lapp32")
      .attribute("process", "chronix")
      .point(Instant.now().toEpochMilli(), 527)
      .point(Instant.now().plusSeconds(60).toEpochMilli(), 683)
      .point(Instant.now().plusSeconds(30).toEpochMilli(), 528)
      .attribute("max", 683)
      .build();
      
//sort the points based on the timestamp
series.sort();
//get as stream of Pairs with index, long, and double (primitive data types)
series.points();
//only get the timestamps
series.getTimestamps()
//only get the values
series.getValues();
```

### Chronix metric time series converter
To use the metric time series with Chronix-Server one need the belonging converter that depends on the Chronix-API.
Both packages are on on Bintray.

```java
SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/chronix/");

 //Define a group by function for the time series records
Function<MetricTimeSeries, String> groupBy = ts -> ts.getMetric() 
                                                   + "-" 
                                                   + ts.attribute("host");

//Define a reduce function for the grouped time series records
BinaryOperator<MetricTimeSeries> reduce = (ts1, ts2) -> {
      MetricTimeSeries.Builder reduced = new MetricTimeSeries
               .Builder(ts1.getMetric())
               .data(concat(ts1.getTimestamps(), ts2.getTimestamps()),
                    concat(ts1.getValues(), ts2.getValues()))
               .attributes(ts2.attributes());
        return reduced.build();
        };
//Instantiate a Chronix Client
ChronixClient<MetricTimeSeries, SolrClient, SolrQuery> chronix = new ChronixClient<>
        (new MetricTimeSeriesConverter(), new ChronixSolrStorage<>(200, groupBy, reduce));

//We want the maximum of all time series that metric matches *load*.
SolrQuery query = new SolrQuery("metric:*Load*");
query.addFilterQuery("ag=max");

//The result is a Java Stream. We simply collect the result into a list.
List<MetricTimeSeries> maxTS = chronix.stream(solr, query).collect(Collectors.toList());
```

## Serialization and Date-Delta-Compaction
Chronix' domain specific Date-Delta-Compaction (DDC) significantly reduces the storage demand.
The central idea of the DDC is: When storing an almost-periodic time series, the DDC keeps track of the expected next timestamp and the actual timestamp.
If the difference is below a threshold, the actual timestamp is not stored as its reconstructed value will be close enough.
Furthermore, the DDC keeps track of the accumulated drift as the difference between the expected timestamps and actual timestamps
adds up with the number of data points stored.
As soon as the drift is above the threshold, DDC stores a correcting delta that brings the reconstruction back to of the actual timestamp.
![ddc](https://cloud.githubusercontent.com/assets/3796738/19962421/c83f5fc0-a1b7-11e6-8c40-08dca99d46fb.png)

DDC calculates deltas (0, 10000, 10002, 10004) of timestamps and compares them (0, 10000, 2, 4).
It removes deltas that are below a threshold (\_, 10000, \_, \_) and checks the drift of the reconstructed timestamps.
As _r4_ is too far off from _t4_, DDC stores a correcting delta instead.
Result: DDC only needs to store two deltas (\_, 10000, \_, 10006) for a reconstruction.


## Usage
Build script snippet for use in all Gradle versions, using the Bintray Maven repository:

```groovy
repositories {
    mavenCentral()
    maven { 
        url "http://dl.bintray.com/chronix/maven" 
    }
}
dependencies {
   compile 'de.qaware.chronix:chronix-timeseries:<latestVersion>'
   compile 'de.qaware.chronix:chronix-timeseries-converter:<latestVersion>'
   compile 'de.qaware.chronix:chronix-timeseries-common:<latestVersion>'
}
 
```


## Contributing

Is there anything missing? Do you have ideas for new features or improvements? You are highly welcome to contribute
your improvements, to the Chronix projects. All you have to do is to fork this repository,
improve the code and issue a pull request.

## Maintainer

Florian Lautenschlager @flolaut

## License

This software is provided under the Apache License, Version 2.0 license.

See the `LICENSE` file for details.
