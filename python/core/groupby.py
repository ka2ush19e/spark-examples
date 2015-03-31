# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

import random

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('GroupBy')
    sc = SparkContext(conf=conf)

    nums = sc.parallelize(range(0, 5), 5).flatMap(
        lambda i: [random.randint(0, 10) for _ in range(0, 1000)]
    ).persist()

    print('### Count ###')
    print(nums.count())
    print()

    print('### Average: reduce ###')
    avg1 = nums.map(lambda n: (n, 1)).reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    print(avg1[0] / avg1[1])
    print()

    print('### Average: fold ###')
    avg2 = nums.map(lambda n: (n, 1)).fold((0, 0), lambda x, y: (x[0] + y[0], x[1] + y[1]))
    print(avg2[0] / avg2[1])
    print()

    print('### Average: aggregate ###')
    avg3 = nums.aggregate(
        (0, 0),
        lambda acc, v: (acc[0] + v, acc[1] + 1),
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
    )
    print(avg3[0] / avg3[1])
    print()

    print('### Grouping: reduceByKey ###')
    grouped = nums.map(lambda n: (n, 1)).reduceByKey(lambda x, y: x + y).sortByKey().collect()
    for (n, c) in grouped:
        print(n, c)
    print()

    print('### Grouping: foldByKey ###')
    grouped = nums.map(lambda n: (n, 1)).foldByKey(0, lambda x, y: x + y).sortByKey().collect()
    for (n, c) in grouped:
        print(n, c)
    print()

    print('### Grouping: aggregateByKey ###')
    grouped = nums.map(lambda n: (n, 1)).aggregateByKey(
        0,
        lambda acc, v: acc + v,
        lambda acc1, acc2: acc1 + acc2
    ).sortByKey().collect()
    for (n, c) in grouped:
        print(n, c)
    print()

    print('### Grouping: combineByKey ###')
    grouped = nums.map(lambda n: (n, 1)).combineByKey(
        lambda v: v,
        lambda acc, v: acc + v,
        lambda acc1, acc2: acc1 + acc2
    ).sortByKey().collect()
    for (n, c) in grouped:
        print(n, c)
    print()

    sc.stop()
