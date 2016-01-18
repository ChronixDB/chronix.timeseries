# Chronix Kassiopeia
Chronix Kassiopeia is library that provides classes and functions to work with time series.

## Usage

### Dependencies

If you are using Maven to build your project, add the following to the `pom.xml` file:
```xml
<repositories>
    <repository>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
        <id>bintray-chronix-maven</id>
        <name>bintray</name>
        <url>http://dl.bintray.com/chronix/maven</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>de.qaware.chronix</groupId>
        <artifactId>chronix-kassiopeia</artifactId>
        <version>0.1.1</version>
    </dependency>
</dependencies>
```

In case you are already using Gradle to build your project, add the following to the `build.gradle` file:
```groovy
repositories {
    mavenCentral()
    maven { 
        url "http://dl.bintray.com/chronix/maven" 
    }
}

dependencies {
	compile 'de.qaware.chronix:chronix-kassiopeia:0.1.1'
}
```

### TimeSeries

A time series is a list of timestamps, each timestamp being a (T, V)-pair
with T (for type of time) a Comparable and no assumptions about V (for type of value). Throughout the package,
the T-values (time) of a time series are assumed non-decreasing.

There are two approaches:
a) Put the list into an object of class TimeSeries. This class guarantees its integrity and provides several useful methods.
b) Forget about the class and apply static methods of TimeSeriesUtil directly to iterators of timestamps, 
   unwinding the input iterators stepwise. This can be an enormous advantage. 

We describe (a) first and then (b).

TimeSeries can be created from iterables or iterators of (time, value)-pairs.
The idea behind this class is mainly that it considers time series stepwise constant functions defined for values of T.
So, the TimeSeries created by the pairs [(0, 7), (10, 70)] would evaluate to null (= undefined) on the interval 
(-oo, 0), to 7 on [0, 10) and to 70 on [10, oo). The pairs [(null, 3), (5, 30)] create a TimeSeries evaluating to 3 
on (-oo, 5) and to 30 on [5, +oo). This shows how TimeSeries implements the method *apply(T)* of UnaryFunction.
The principle of left closed and right open intervals holds throughout the class.
TimeSeries has been designed for handling -oo and +oo as valid time and for coping with missing values
which are represented by null. All time series go from -oo to +oo, the first value
might be defined (not null) or undefined (null). null is considered the smallest element of the universe.
This class guarantees:

 * T-value of first timestamp =  -oo (represented by null),

 * T-values are strictly ascending.

 * The value changes at each timestamp.

The crucial methods of TimeSeries are three varieties of merge. What they have in common is to
create a new TimeSeries from the ones given by merging (unioning) all timestamps. They differ in how they compute
the new value from the values available. They are all static.

*merge(TimeSeries\<V\> tv, TimeSeries\<V\> tw, BinaryOperator\<V, V, U\>)*

*merge(Iterable\<TimeSeries\>, BinaryOperator\<V\>)*

*merge(Iterable\<TimeSeries\>)*

The first *merge* applies the BinaryOperator to the two values available. Applying
this method to two Integer-valued TimeSeries tv and tw with (x, y) -> x < y as BinaryOperator yields
a Boolean-valued TimeSeries indicating where tv is less than tw. With tv created by
[(0, 7), (10, 70)] and tw by [(null, 3), (5, 30)] the result would be
[(null, True), (0, False), (5, True), (10, False)].

The second *merge* accepts any number of TimeSeries as an Iterable and reduces the available values
to one using the given Binary Operator.

The third *merge* returns the vector of available values.

Other methods are *size()*, *subSeries(T, T)*, *relocate(Iteraor\<T\>)* and *sameLeg(T, T)*.
*size* returns the number of timestamps, *subSeries* returns a TimeSeries undefined outside of the given borders, 
*relocate* replaces the existing timestamps with the ones given and
*sameLeg* returns false if the value changes between the two points. 

Some examples:
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

We now turn to TimeSeriesUtil. *cleanse* behaves exactly as the constructor of TimeSeries: it removes all but the last
of timestamps with equal t (time) and all but the first of timestamps with equal v (value). *compact* could be used to
replace seconds-based mesures with minutes-based values of the avg, min or max within that minute. 
The last method method *linearize* computes a stepwise linear approximation of the given time series with epsilon 
(the last argument) being the maximum deviation. *linearize* uses SimpleRegression of apache.commons and works only 
for time series of <Double, Double>.

Some examples:
```java

		Iterator<Double> integers = unaryGenerator(0.0, x -> x + 1.0);   // the integers

        // ts (any time series) is averaged to integer 
        Iterator<Pair<Double, Double>> result = TimeSeriesUtil.compact(ts.iterator(), integers, avg);
        
        
        // ts contains two adjacent segments of similar slope
        List<Pair<Double, Double>> ts = new ArrayList<>();
        
        double epsilon = 1e-3;
        double slope = 0.90;
        
        ts.add(Pair.pairOf(0.0, 0.0));              // first segment, slope = 1.0
        ts.add(Pair.pairOf(1.0, 1.0));
        ts.add(Pair.pairOf(2.0, 2.0));
        ts.add(Pair.pairOf(3.0, 3.0));
        
        ts.add(Pair.pairOf(4.0, 3.0 + 1 * slope));    // second segment, slope = slope
        ts.add(Pair.pairOf(5.0, 3.0 + 2 * slope));
        ts.add(Pair.pairOf(6.0, 3.0 + 3 * slope));
        ts.add(Pair.pairOf(7.0, 3.0 + 4 * slope));
        
        // call linearize
        Iterator<Pair<Double, Pair<Double, Double>>> result = TimeSeriesUtil.linearize(ts.iterator(), epsilon);
```
## Maintainer

Johannes Siedersleben (@johsieders)

Florian Lautenschlager (@flolaut)

## License

This software is provided under the Apache License Version 2, read the `LICENSE.txt` file for details.