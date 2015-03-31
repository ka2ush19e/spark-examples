package core;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PairRddAction {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Pair Rdd Action");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, String> pairs = sc.parallelizePairs(Arrays.asList(
            new Tuple2<Integer, String>(1, "a"),
            new Tuple2<Integer, String>(1, "b"),
            new Tuple2<Integer, String>(1, "c"),
            new Tuple2<Integer, String>(2, "d"),
            new Tuple2<Integer, String>(2, "e")
        ));

        System.out.println("### keys ###");
        System.out.println(pairs.keys().collect());
        System.out.println();

        System.out.println("### values ###");
        System.out.println(pairs.values().collect());
        System.out.println();

        System.out.println("### countByKey ###");
        for (Map.Entry<Integer, Object> e : new TreeMap<Integer, Object>(pairs.countByKey()).entrySet()) {
            System.out.println(String.format("%d: %s", e.getKey(), e.getValue()));
        }
        System.out.println();

        System.out.println("### collectAsMap ###");
        for (Map.Entry<Integer, String> e : new TreeMap<Integer, String>(pairs.collectAsMap()).entrySet()) {
            System.out.println(String.format("%d: %s", e.getKey(), e.getValue()));
        }
        System.out.println();

        System.out.println("### lookup ###");
        System.out.println("1: " + pairs.lookup(1));
        System.out.println("5: " + pairs.lookup(5));
        System.out.println();

        sc.stop();
    }
}
