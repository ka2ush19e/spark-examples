# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, IntegerType

if __name__ == '__main__':
    conf = SparkConf().setAppName('Restaurants Parquet')
    sc = SparkContext(conf=conf)
    hive_ctx = HiveContext(sc)

    inputs = hive_ctx.parquetFile(sys.argv[1])
    inputs.registerTempTable('restaurants')

    hive_ctx.registerFunction("LEN", lambda s: len(s), IntegerType())

    print('### Schema ###')
    inputs.printSchema()
    print()

    print('### Restaurants in Tokyo ###')
    restaurants_in_tokyo = hive_ctx.sql("""
        SELECT
            r.id,
            r.alphabet
        FROM
            restaurants r
        WHERE
            r.pref_id = '13'
        AND r.alphabet <> ''
        LIMIT
            10
    """).collect()

    for r in restaurants_in_tokyo:
        print(r.id, r.alphabet)
