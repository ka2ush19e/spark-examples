# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('SequenceFile Operation')
    sc = SparkContext(conf=conf)

    saveData = sc.parallelize([(1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e')]).persist()

    print('### SequenceFile ###')
    saveData.saveAsSequenceFile('output_sequence')

    loadDataFromSequenceFile = sc.sequenceFile('output_sequence',
                                               'org.apache.hadoop.io.IntWritable',
                                               'org.apache.hadoop.io.Text')
    for e in loadDataFromSequenceFile.collect():
        print('{}: {}'.format(e[0], e[1]))
    print()

    print('### ObjectFile ###')
    saveData.saveAsPickleFile("output_object")

    loadDataFromObjectFile = sc.pickleFile("output_object")
    for e in loadDataFromObjectFile.collect():
        print('{}: {}'.format(e[0], e[1]))
    print()

    print('### HadoopFile ###')
    saveData.saveAsHadoopFile('output_hadoop',
                              'org.apache.hadoop.mapred.TextOutputFormat',
                              'org.apache.hadoop.io.IntWritable',
                              'org.apache.hadoop.io.Text')

    loadDataFromHadoopFile = sc.hadoopFile("output_hadoop",
                                           'org.apache.hadoop.mapred.KeyValueTextInputFormat',
                                           'org.apache.hadoop.io.IntWritable',
                                           'org.apache.hadoop.io.Text')
    for e in loadDataFromHadoopFile.collect():
        print('{}: {}'.format(e[0], e[1]))
    print()

    # print('### Compressed HadoopFile ###')
    # saveData.saveAsHadoopFile('output_hadoop_compressed',
    #                           'org.apache.hadoop.mapred.TextOutputFormat',
    #                           'org.apache.hadoop.io.IntWritable',
    #                           'org.apache.hadoop.io.Text',
    #                           'org.apache.hadoop.io.compress.SnappyCodec')
    #
    # loadDataFromCompressedHadoopFile = sc.hadoopFile("output_hadoop_compressed",
    #                                                  'org.apache.hadoop.mapred.KeyValueTextInputFormat',
    #                                                  'org.apache.hadoop.io.IntWritable',
    #                                                  'org.apache.hadoop.io.Text')
    # for e in loadDataFromCompressedHadoopFile.collect():
    #     print('{}: {}'.format(e[0], e[1]))
    # print()

    sc.stop()
