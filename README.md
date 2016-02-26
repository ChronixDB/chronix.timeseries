[![Build Status](https://travis-ci.org/ChronixDB/chronix.kassiopeia.svg)](https://travis-ci.org/ChronixDB/chronix.kassiopeia)
[![Coverage Status](https://coveralls.io/repos/ChronixDB/chronix.kassiopeia/badge.svg?branch=master&service=github)](https://coveralls.io/github/ChronixDB/chronix.kassiopeia?branch=master)
[![Stories in Ready](https://badge.waffle.io/ChronixDB/chronix.kassiopeia.png?label=ready&title=Ready)](https://waffle.io/ChronixDB/chronix.kassiopeia)
[![Apache License 2](http://img.shields.io/badge/license-ASF2-blue.svg)](https://github.com/ChronixDB/chronix.kassiopeia/blob/master/LICENSE)

# Chronix Kassiopeia
Chronix Kassiopeia is a library that provides classes and functions to work with time series.
Kassiopeia can easily plugged into Chronix.
Kassiopeia is split up into two different time series classes with its belonging converters.
The first implementation is Chronix-Kassiopeia. 
It consists of the following packages: 
- Chronix-Kassiopeia
- Chronix-Kassiopeia-Converter

The second implementation is Chronix-Kassiopeia-Simple.
- Chronix-Kassiopeia-Simple
- Chronix-Kassiopeia-Simple-Converter

## Chronix-Kassiopeia
Chronix-Kassiopeia has no dependencies to Chronix.
Hence it can be used in any project without putting any Chronix libraries to the classpath.

### Chronix-Kassiopeia Time Series
That's how Chronix-Kassiopeia time series looks:
(A detailed description could be found here:
[Chronix-Kassiopeia](https://github.com/ChronixDB/chronix.kassiopeia/tree/master/chronix-kassiopeia))
```java
	ArrayList<Pair<Integer, Integer>> aux = new ArrayList<>();
	aux.add(pairOf(0, 7));
	aux.add(pairOf(10, 70));
	TimeSeries<Integer, Integer> tv = new TimeSeries<>(aux);
	sf.apply(-10000);          // returns null
	sf.apply(0);               // returns 7
	sf.apply(10);              // returns 70
	sf.apply(1000);            // returns 70

	aux.clear();
	aux.add(pairOf(null, 3));
	aux.add(pairOf(5, 30));
	TimeSeries<Integer, Integer> tw = new TimeSeries<>(aux);

	TimeSeries<Integer, Boolean> tr = merge(tv, tw, (x, y) -> x < y);
	tr.apply(-10000);          // returns True
	tr.apply(0);               // returns False
	tr.apply(5);               // returns True
	tr.apply(10);              // returns False
```

### Chronix-Kassiopeia Time Series Converter
To use Chronix-Kassiopeia with Chronix-Server one need the Chronix-Kassiopeia-Converter and the Chronix-API package. 
Both packages are on on Bintray.

This is a code snipped that shows how to integrate Chronix-Kassiopeia and Chronix-Server:
```Java
//Define a group by function for the time series records
Function<TimeSeries<Long, Double>, String> groupBy = ts -> ts.getAttribute("metric") 
                                                           + "-"
                                                           + ts.getAttribute("host");

//Define a reduce function for the grouped time series records. We use the average.
BinaryOperator<TimeSeries<Long, Double>> reduce = (ts1, ts2) -> 
                                                   merge(ts1, ts2, (y1, y2) -> (y1 + y2) / 2);

//Instantiate a Chronix Client
ChronixClient<TimeSeries<Long, Double>, SolrClient, SolrQuery> chronix = new ChronixClient<>
                    (new KassiopeiaConverter(), new ChronixSolrStorage<>(200, groupBy, reduce));

//We want all time series that metric matches *load*.
SolrQuery query = new SolrQuery("metric:*Load*");

//The result is a Java Stream. We simply collect the result into a list.
List<TimeSeries<Long, Double>> result = chronix.stream(solr, query).collect(Collectors.toList());
```

## Chronix-Kassiopeia-Simple
As Chronix-Kassiopeia the Chronix-Kassiopeia-Simple package also does not depend on any other Chronix library.

**Note:** The Chronix-Server uses for server-side aggregations and high-level analyses the Chronix-Kassiopeia-Simple time series.
Hence one currently can only use server-side functions with Chronix-Kassiopeia-Simple. 
### Chronix-Kassiopeia-Simple
That's how Chronix-Kassiopeia-Simple time series looks:
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

### Chronix-Kassiopeia-Simple-Converter
To use Chronix-Kassiopeia-Simple with Chronix-Server one need the Chronix-Kassiopeia-Simple-Converter and the Chronix-API package.
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
        (new KassiopeiaSimpleConverter(), new ChronixSolrStorage<>(200, groupBy, reduce));

//We want the maximum of all time series that metric matches *load*.
SolrQuery query = new SolrQuery("metric:*Load*");
query.addFilterQuery("ag=max");

//The result is a Java Stream. We simply collect the result into a list.
List<MetricTimeSeries> maxTS = chronix.stream(solr, query).collect(Collectors.toList());
```



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
   compile 'de.qaware.chronix:chronix-kassiopeia:0.1.4'
   compile 'de.qaware.chronix:chronix-kassiopeia-converter:0.1.4'
   //or use the simple time series class
   compile 'de.qaware.chronix:chronix-kassiopeia-simple:0.1.4'
   compile 'de.qaware.chronix:chronix-kassiopeia-simple-converter:0.1.4'
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
