package de.qaware.chronix.timeseries.dt

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for the list util class.
 * @author f.lautenschlager
 */
class ListUtilTest extends Specification {
    @Unroll
    def "test rangeCheck for size: #size, index: #index, exception thrown: #expected"() {
        when:
        boolean thrown = false
        try {
            ListUtil.rangeCheck(index, size)
        } catch (IndexOutOfBoundsException e) {
            thrown = true;
        }
        then:
        thrown == expected

        where:
        index << [1, 2]
        size << [2, 1]
        expected << [false, true]
    }

    @Unroll
    def "test rangeCheckAdd for size: #size, index: #index, exception thrown: #expected"() {
        when:
        boolean thrown = false
        try {
            ListUtil.rangeCheckForAdd(index, size)
        } catch (IndexOutOfBoundsException e) {
            thrown = true;
        }
        then:
        thrown == expected

        where:
        index << [1, 2, -1]
        size << [2, 1, 2]
        expected << [false, true, true]
    }

    @Unroll
    def "test calculateNewCapacity for oldCapacity: #oldCapacity with a minCapacity of: #minCapacity. Expecting: #expectedCapacity"() {
        when:
        def newCapacity = ListUtil.calculateNewCapacity(oldCapacity, minCapacity)
        then:
        newCapacity == expectedCapacity
        where:
        oldCapacity << [1, 2, Integer.MAX_VALUE - 8]
        minCapacity << [2, 1, Integer.MAX_VALUE - 7]
        expectedCapacity << [2, -1, 2147483647]
    }

    @Unroll
    def "test hugeCapacity for minCapacity: #minCapacity, expected: #expectedCapacity, exception thrown: #exception"() {
        when:
        def thrown = false
        def capacity = -1;
        try {
            capacity = ListUtil.hugeCapacity(minCapacity)
        } catch (OutOfMemoryError e) {
            thrown = true
        }

        then:
        capacity == expectedCapacity
        thrown == exception

        where:
        minCapacity << [1, -1]
        expectedCapacity << [2147483639, -1]
        exception << [false, true]
    }

    def "test private constructor"() {
        when:
        ListUtil.newInstance()
        then:
        noExceptionThrown()
    }
}
