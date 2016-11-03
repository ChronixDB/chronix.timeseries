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
package de.qaware.chronix.timeseries.dts;


import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static de.qaware.chronix.timeseries.dts.Functions.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Basic Functions test.
 */
public class FunctionsTest {

    @Test
    public void testCompose() {
        Function<Pair<String, Integer>, Integer> f = Pair::getSecond;
        Function<Integer, Integer> g = x -> 2 * x;
        Function<Pair<String, Integer>, Integer> h = compose(g, f);
        Pair<String, Integer> p = new Pair<>("hello", 7);
        int s = h.apply(p);
        assertEquals(14, s);
    }

    @Test
    public void testCurry() {
        Function<List<Integer>, Integer> sum = FunctionsTest::sum;

        List<Integer> args = asList(1, 2, 3, 4, 5, null);

        Function<List<Integer>, Integer> sum100 = curryLeft(sum, Collections.singletonList(100));
        Integer s = sum100.apply(args);
        assertEquals(115, s.intValue());

        Function<List<Integer>, Integer> sum200 = curryRight(sum, Collections.singletonList(200));
        s = sum200.apply(args);
        assertEquals(215, s.intValue());

        sum200 = curryRight(sum, Collections.singletonList(0));
        s = sum200.apply(args);
        assertEquals(15, s.intValue());

        Function<Integer, Integer> add7 = curryLeft((x, y) -> x + y, 7);
        assertEquals(Integer.valueOf(15), add7.apply(8));

        Function<Integer, Integer> add9 = curryRight((x, y) -> x + y, 9);
        assertEquals(Integer.valueOf(17), add9.apply(8));
    }

    private static Integer sum(List<Integer> ints) {
        Integer sum = 0;
        for (Integer i : ints) {
            if (i != null) {
                sum = sum + i;
            }
        }
        return sum;
    }
}






