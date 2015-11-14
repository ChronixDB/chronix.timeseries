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
package de.qaware.chronix.dts;


import org.junit.Test;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * Basic pair unit test.
 */
public class PairTest {

    @Test
    public void testNormalUseScenario() {
        Pair<Integer, String> pair = new Pair<>(12, "Hallo");
        assertTrue(pair.toString().contains("12"));
        assertTrue(pair.toString().contains("Hallo"));

        Pair<Integer, String> pair2 = new Pair<>(12, "Hallo");
        Pair<Integer, String> pair3 = new Pair<>(13, "Hallo");
        Pair<Integer, String> pair4 = new Pair<>(12, "Hallo2");
        assertEquals(pair, pair2);
        assertFalse(pair.equals(pair3));
        assertFalse(pair.equals(pair4));

        assertEquals(pair.hashCode(), pair2.hashCode());

        assertSame(pair.getFirst(), 12);
        assertSame(pair.getSecond(), "Hallo");

        assertTrue(pair.asArray()[0].equals(12));
        assertTrue(pair.asArray()[1].equals("Hallo"));

        assertTrue(pair.asList().get(0).equals(12));
        assertTrue(pair.asList().get(1).equals("Hallo"));
    }

    @Test
    public void testNullHandling() {
        Pair<Integer, String> pair = new Pair<>(null, null);
        assertEquals(pair.toString(), "");
        assertNull(pair.getFirst());
        assertNull(pair.getSecond());
    }

}