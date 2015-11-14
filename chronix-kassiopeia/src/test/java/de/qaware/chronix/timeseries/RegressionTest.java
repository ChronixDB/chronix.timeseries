/*
 *    Copyright (C) 2015 QAware GmbH
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
package de.qaware.chronix.timeseries;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.junit.Test;

/**
 * Basic unit test for a regression.
 */
public class RegressionTest {

    @Test
    public void testRegression() {

        SimpleRegression regression = new SimpleRegression();
        regression.addData(0.0, 1.0);
        regression.addData(1.0, 2.5);
        regression.addData(2.0, 3.0);

        double slope = regression.getSlope();
        double intercept = regression.getIntercept();
        long n = regression.getN();
        double err = regression.getMeanSquareError();
    }
}
