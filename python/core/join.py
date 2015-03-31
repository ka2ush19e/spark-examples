# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('Join')
    sc = SparkContext(conf=conf)

    persons = sc.parallelize([("Adam", "San francisco"),
                              ("Bob", "San francisco"),
                              ("Taro", "Tokyo"),
                              ("Charles", "New York")])

    cities = sc.parallelize([("Tokyo", "Japan"),
                             ("San francisco", "America"),
                             ("Beijing", "China")])

    print("### Join ###")
    joined = persons.map(lambda (name, city): (city, name)).join(cities).collect()
    for (city, (name, country)) in joined:
        print("{:<10} {:<10} {:<10}".format(name, country, city))
    print()

    print("### Left outer join ###")
    joined = persons.map(lambda (name, city): (city, name)).leftOuterJoin(cities).collect()
    for (city, (name, country)) in joined:
        print("{:<10} {:<10} {:<10}".format(name, country or '', city))
    print()

    print("### Right outer join ###")
    joined = persons.map(lambda (name, city): (city, name)).rightOuterJoin(cities).collect()
    for (city, (name, country)) in joined:
        print("{:<10} {:<10} {:<10}".format(name or '', country, city))
    print()

    print("### Full outer join ###")
    joined = persons.map(lambda (name, city): (city, name)).fullOuterJoin(cities).collect()
    for (city, (name, country)) in joined:
        print("{:<10} {:<10} {:<10}".format(name or '', country or '', city))
    print()

    print("### Grouping ###")
    grouped = persons.map(lambda (name, city): (city, name)).cogroup(cities).collect()
    for (city, (names, countries)) in grouped:
        print("{:<20} {:<20} {:<20}".format(city, ",".join(names), ",".join(countries)))
    print()

    sc.stop()
