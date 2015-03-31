# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row

if __name__ == '__main__':
    conf = SparkConf().setAppName('Restaurants')
    sc = SparkContext(conf=conf)
    hive_ctx = HiveContext(sc)

    restaurants = (
        sc.textFile(sys.argv[1])
        .filter(lambda l: not l.startswith('id'))
        .map(lambda l: l.split(','))
        .filter(lambda r: len(r) >= 7)
        .map(lambda r: Row(id=r[0], name=r[1], property=r[2], alphabet=r[3], name_kana=r[4],
                           pref_id=r[5], area_id=r[6]))
    )
    hive_ctx.inferSchema(restaurants).registerTempTable('restaurants')

    prefs = (
        sc.textFile(sys.argv[2])
        .filter(lambda l: not l.startswith('id'))
        .map(lambda l: l.split(','))
        .filter(lambda r: len(r) >= 2)
        .map(lambda r: Row(id=r[0], name=r[1]))
    )
    hive_ctx.inferSchema(prefs).registerTempTable('prefs')

    areas = (
        sc.textFile(sys.argv[3])
        .filter(lambda l: not l.startswith('id'))
        .map(lambda l: l.split(','))
        .filter(lambda r: len(r) >= 3)
        .map(lambda r: Row(id=r[0], pref_id=r[1], name=r[2]))
    )
    hive_ctx.inferSchema(areas).registerTempTable('areas')

    print('### Restaurants in Tokyo ###')
    restaurants_in_tokyo = hive_ctx.sql("""
        SELECT
            r.id,
            r.alphabet,
            p.name
        FROM
            restaurants r
        INNER JOIN prefs p
            ON p.id = r.pref_id
        WHERE
            p.id = '13'
        AND r.alphabet <> ''
        LIMIT
            10
    """).collect()
    for r in restaurants_in_tokyo:
        print(r.id, r.alphabet, r.name.encode('utf-8'))
