# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

from pyspark import SparkConf, SparkContext, SparkFiles


if __name__ == '__main__':
    conf = SparkConf().setAppName('Pipe')
    sc = SparkContext(conf=conf)

    column_count_script = './scripts/columncount.py'
    column_count_script_name = 'columncount.py'
    sc.addFile(column_count_script)

    lines = sc.parallelize(['1,2,3', '4,5', '6', '7,8,9,10'])
    print(lines.pipe(SparkFiles.get(column_count_script_name)).collect())

    sc.stop()
