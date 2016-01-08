package de.qaware.chronix.timeseries

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for our DoubleList
 * @author f.lautenschlager
 */
class DoubleListTest extends Specification {
    def "test size"() {
        given:
        def list = new DoubleList()

        when:
        list.add(1d)
        list.add(2d)

        list.remove(2d)
        then:
        list.size() == 1
    }

    def "test isEmpty"() {
        when:
        def list = insertedList

        then:
        list.isEmpty() == expected

        where:
        insertedList << [new DoubleList()]
        expected << [true]
    }

    @Unroll
    def "test contains value: #values expected: #expected"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.contains(values)
        result == expected

        where:
        values << [3d, 4d, 5d, 6d, 2d]
        expected << [true, true, true, false, false]
    }

    @Unroll
    def "test indexOf value: #values index: #expected"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.indexOf(values)
        result == expected

        where:
        values << [3d, 4d, 5d, 6d]
        expected << [0, 1, 3, -1]
    }

    @Unroll
    def "test lastIndexOf value: #values index: #expected"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.lastIndexOf(values)
        result == expected

        where:
        values << [3d, 4d, 5d, 6d]
        expected << [0, 2, 3, -1]
    }

    def "test copy"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.copy()
        result.equals(list)

    }

    def "test toArray"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.toArray()
        result == [3d, 4d, 4d, 5d] as double[]
    }

    def "test get"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.get(index)
        result == expected

        where:
        index << [3, 2, 1, 0]
        expected << [5d, 4d, 4d, 3d]

    }

    def "test set"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        def result = list.set(index, 4)
        result == expected
        list.set(index, 4) == 4

        where:
        index << [3, 2, 1, 0]
        expected << [5d, 4d, 4d, 3d]
    }


    def "test add"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        then:
        list.size() == 4
        list.contains(3d)
        list.contains(4d)
        list.contains(4d)
        list.contains(5d)
    }


    def "test add at index"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        list.add(3, 99d)

        then:
        list.size() == 5
        list.get(3) == 99d
    }

    def "test remove double"() {
        given:
        def list = new DoubleList()


        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        def firstRemove = list.remove(4d)
        def secondRemove = list.remove(1d)

        then:
        firstRemove
        !secondRemove
        list.size() == 3
        list.get(1) == 4d
    }


    def "test remove at index"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        def result = list.remove(3)

        then:
        result == 5d
        list.size() == 3
    }

    def "test clear"() {
        given:
        def list = new DoubleList()

        when:
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)

        list.clear()
        then:
        list.size() == 0
    }

    def "test addAll"() {
        given:
        def list1 = new DoubleList()
        def list2 = new DoubleList()

        when:
        list1.add(1d)
        list1.add(2d)
        list1.add(3d)


        list2.add(3d)
        list2.add(4d)
        list2.add(4d)
        list2.add(5d)

        def result = list1.addAll(list2)
        then:
        result
        list1.size() == 7
        list1.contains(3d)
        list1.contains(4d)
        list1.contains(5d)

    }


    def "test addAll at index"() {
        given:
        def list1 = new DoubleList()
        def list2 = new DoubleList()

        when:
        list1.add(1d)
        list1.add(2d)
        list1.add(3d)


        list2.add(3d)
        list2.add(4d)
        list2.add(4d)
        list2.add(5d)

        def result = list1.addAll(3, list2)
        then:
        result
        list1.size() == 7
        list1.get(0) == 1d
        list1.get(1) == 2d
        list1.get(2) == 3d
        list1.get(3) == 3d
        list1.get(4) == 4d
        list1.get(5) == 4d
        list1.get(6) == 5d

    }

    def "test removeRange"() {
        given:
        def list = new DoubleList()

        when:
        list.add(1d)
        list.add(2d)
        list.add(3d)
        list.add(3d)
        list.add(4d)
        list.add(4d)
        list.add(5d)


        list.removeRange(3, 6)

        then:
        list.size() == 4
        !list.contains(4d)
    }

    def "test hash code"() {
        given:
        def list = new DoubleList()
        def list2 = new DoubleList()

        when:
        list.add(1d)
        list.add(2d)
        list.add(3d)

        list2.add(1d)
        list2.add(2d)
        list2.add(3d)


        then:
        list.hashCode() == list2.hashCode()

    }

    def "test to string"() {
        given:
        def list = new DoubleList()
        list.add(1d)

        when:
        def toString = list.toString()

        then:
        toString != null
        toString.size() > 0
    }

    def "test equals"() {
        given:
        def list = new DoubleList()
        list.add(1d)
        list.add(2d)
        list.add(3d)

        when:
        def result = list.equals(other)
        def alwaysTrue = list.equals(list)

        then:
        alwaysTrue
        result == expected

        where:
        other << [null, new Integer(1), new DoubleList()]
        expected << [false, false, false]
    }

    def "test constructor"() {

        when:
        def list = new DoubleList(initialSize)

        then:
        list.elementData.size() == initialSize

        where:
        initialSize << [5, 0]
    }

    def "test invalid initial size"() {
        when:
        new DoubleList(-1)
        then:
        thrown IllegalArgumentException
    }

    def "test index out of bound exceptions"() {

        when:
        c1.call()


        then:
        thrown IndexOutOfBoundsException

        where:
        c1 << [{ new DoubleList().get(0) } as Closure, { new DoubleList().add(-1, 0) } as Closure]

    }


}
