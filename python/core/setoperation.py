# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('Set Operation')
    sc = SparkContext(conf=conf)

    nums1 = sc.parallelize([5, 5, 4, 4, 3, 3, 2, 2, 1, 1])
    nums2 = sc.parallelize([5, 3, 3, 1, 1])

    print("### Distinct ###")
    print(",".join([str(n) for n in nums1.distinct().collect()]))
    print()

    print("### Union ###")
    print(",".join([str(n) for n in nums1.union(nums2).collect()]))
    print()

    print("### Intersection ###")
    print(",".join([str(n) for n in nums1.intersection(nums2).collect()]))
    print()

    print("### Subtract ###")
    print(",".join([str(n) for n in nums1.subtract(nums2).collect()]))
    print()

    print("### Cartesian ###")
    print(",".join([str(n) for n in nums1.cartesian(nums2).collect()]))
    print()
