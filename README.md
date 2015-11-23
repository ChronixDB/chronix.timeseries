[![Build Status](https://travis-ci.org/ChronixDB/chronix.kassiopeia.svg)](https://travis-ci.org/ChronixDB/chronix.kassiopeia)
[![Coverage Status](https://coveralls.io/repos/ChronixDB/chronix.kassiopeia/badge.svg?branch=master&service=github)](https://coveralls.io/github/ChronixDB/chronix.kassiopeia?branch=master)
[![Stories in Ready](https://badge.waffle.io/ChronixDB/chronix.kassiopeia.png?label=ready&title=Ready)](https://waffle.io/ChronixDB/chronix.kassiopeia)
[![Apache License 2](http://img.shields.io/badge/license-ASF2-blue.svg)](https://github.com/ChronixDB/chronix.kassiopeia/blob/master/LICENSE)

# Chronix Kassiopeia

A time series framework that can be used within Chronix.


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
   compile 'de.qaware.chronix:chronix-kassiopeia:0.0.1'
   compile 'de.qaware.chronix:chronix-kassiopeia-converter:0.0.1'
   //or use the simple time series class
   compile 'de.qaware.chronix:chronix-kassiopeia-simple:0.0.1'
   compile 'de.qaware.chronix:chronix-kassiopeia-simple-converter:0.0.1'
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
