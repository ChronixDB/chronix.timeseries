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
package de.qaware.chronix.dts;


import org.junit.Test;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static de.qaware.chronix.dts.WeakLogic.*;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;


/**
 * Basic WeakLogic test.
 */
public class WeakLogicTest {

    @Test
    public void testWeakComparator() {
        Comparator<Integer> cmp = (x, y) -> x - y;

        Comparator<Integer> w = weakComparator(cmp);

        assertTrue(w.compare(0, 1) < 0);
        assertTrue(w.compare(1, 0) > 0);
        assertSame(w.compare(1, 1), 0);
        assertTrue(w.compare(null, 1) < 0);
        assertTrue(w.compare(1, null) > 0);
        assertSame(w.compare(null, null), 0);

        w = weakComparator();

        assertTrue(w.compare(0, 1) < 0);
        assertTrue(w.compare(1, 0) > 0);
        assertSame(w.compare(1, 1), 0);
        assertTrue(w.compare(null, 1) < 0);
        assertTrue(w.compare(1, null) > 0);
        assertSame(w.compare(null, null), 0);

        cmp = weakComparator();

        int result;

        result = cmp.compare(null, null);
        assertEquals(result, 0);
        result = cmp.compare(null, 1);
        assertEquals(result, -1);
        result = cmp.compare(1, 2);
        assertEquals(result, -1);
        result = cmp.compare(1, null);
        assertEquals(result, 1);
    }


    @Test
    public void testWeakBinaryOperator() {
        BinaryOperator<Integer> weakAdd = weakBinaryOperator((Integer a, Integer b) -> a + b);
        Integer sum;

        sum = weakAdd.apply(null, null);
        assertNull(sum);

        sum = weakAdd.apply(7, null);
        assertEquals(Integer.valueOf(7), sum);

        sum = weakAdd.apply(null, 8);
        assertEquals(Integer.valueOf(8), sum);

        sum = weakAdd.apply(7, 8);
        assertEquals(Integer.valueOf(15), sum);
    }

    @Test
    public void testWeakUnaryOperator() {
        UnaryOperator<Integer> weak2 = weakUnaryOperator(x -> 2 * x);
        int s = weak2.apply(5);
        assertEquals(10, s);
        assertNull(weak2.apply(null));
    }

    @Test
    public void testWeakFunction() {
        Function<Integer, Integer> weak2 = weakFunction(x -> 2 * x);
        int s = weak2.apply(5);
        assertEquals(10, s);
        assertNull(weak2.apply(null));
    }

    @Test
    public void testWeakEquals() {
        boolean b = weakEquals(null, null);
        assertTrue(b);

        b = weakEquals("xxx", null);
        assertFalse(b);

        b = weakEquals(null, "xxx");
        assertFalse(b);

        b = weakEquals("xxx", "xxx");
        assertTrue(b);
    }
}






