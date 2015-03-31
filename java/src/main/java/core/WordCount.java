package core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

import scala.Tuple2;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

public class WordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Word Count");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Set<String> stopWordSet = new HashSet<String>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/stopwords.txt"));
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                stopWordSet.add(line);
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
        final Broadcast<Set<String>> stopWords = sc.broadcast(stopWordSet);
        final Accumulator<Integer> stopWordCount = sc.accumulator(0);

        JavaPairRDD<String, Long> wordCounts = sc.textFile(args[0])
            .flatMap(new FlatMapFunction<String, String>() {
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split("[\\s,.:;\'\"?!\\-\\(\\)\\[\\]\\{\\}]"));
                }
            })
            .map(new Function<String, String>() {
                public String call(String s) throws Exception {
                    return s.toLowerCase();
                }
            })
            .filter(new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    return s.length() > 1;
                }
            })
            .filter(new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    if (stopWords.value().contains(s)) {
                        stopWordCount.add(1);
                        return false;
                    }
                    return true;
                }
            })
            .mapToPair(new PairFunction<String, String, Long>() {
                public Tuple2<String, Long> call(String s) throws Exception {
                    return new Tuple2<String, Long>(s, 1L);
                }
            })
            .reduceByKey(new Function2<Long, Long, Long>() {
                public Long call(Long x, Long y) throws Exception {
                    return x + y;
                }
            })
            .persist(StorageLevel.MEMORY_AND_DISK_SER());

        final Long count = wordCounts
            .mapToDouble(new DoubleFunction<Tuple2<String, Long>>() {
                @Override
                public double call(Tuple2<String, Long> v) throws Exception {
                    return v._2().doubleValue();
                }
            })
            .sum().longValue();

        System.out.println("### All words count ###");
        System.out.println(count);
        System.out.println();

        System.out.println("### Stopped word count ###");
        System.out.println(stopWordCount.value());
        System.out.println();

        System.out.println("### Ranking ###");
        List<Tuple2<String, Long>> ranks = wordCounts
            .mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                public Tuple2<Long, String> call(Tuple2<String, Long> t) throws Exception {
                    return new Tuple2<Long, String>(t._2(), t._1());
                }
            })
            .sortByKey(false)
            .mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
                public Tuple2<String, Long> call(Tuple2<Long, String> t) throws Exception {
                    return new Tuple2<String, Long>(t._2(), t._1());
                }
            })
            .take(10);
        for (Tuple2<String, Long> r : ranks) {
            System.out.println(r._1() + " " + r._2());
        }
        System.out.println();

        System.out.println("### Ranking and rate ###");
        List<Tuple2<String, Tuple2<Long, Double>>> rankRates = wordCounts
            .mapValues(new Function<Long, Tuple2<Long, Double>>() {
                public Tuple2<Long, Double> call(Long v) throws Exception {
                    return new Tuple2<Long, Double>(v, v * 1.0 / count * 100);
                }
            })
            .mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Double>>, Tuple2<Long, Double>, String>() {
                public Tuple2<Tuple2<Long, Double>, String> call(Tuple2<String, Tuple2<Long, Double>> t) throws Exception {
                    return new Tuple2<Tuple2<Long, Double>, String>(t._2(), t._1());
                }
            })
            .sortByKey(new TupleComparator(), false)
            .mapToPair(new PairFunction<Tuple2<Tuple2<Long, Double>, String>, String, Tuple2<Long, Double>>() {
                public Tuple2<String, Tuple2<Long, Double>> call(Tuple2<Tuple2<Long, Double>, String> t) throws Exception {
                    return new Tuple2<String, Tuple2<Long, Double>>(t._2(), t._1());
                }
            })
            .take(10);

        for (Tuple2<String, Tuple2<Long, Double>> r : rankRates) {
            System.out.println(r._1() + " " + r._2()._1() + " " + r._2()._2());
        }

        sc.stop();
    }

    private static class TupleComparator implements Comparator<Tuple2<Long, Double>>, Serializable {
        @Override
        public int compare(Tuple2<Long, Double> v1, Tuple2<Long, Double> v2) {
            return (int) (v1._1() - v2._1());
        }
    }
}
