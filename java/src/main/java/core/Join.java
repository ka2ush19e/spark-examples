package core;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class Join {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Join");
        JavaSparkContext sc = new JavaSparkContext(conf);

        @SuppressWarnings("unchecked")
        JavaPairRDD<String, String> persons = sc.parallelizePairs(Arrays.asList(
            new Tuple2<String, String>("Adam", "San francisco"),
            new Tuple2<String, String>("Bob", "San francisco"),
            new Tuple2<String, String>("Taro", "Tokyo"),
            new Tuple2<String, String>("Charles", "New York")
        ));

        @SuppressWarnings("unchecked")
        JavaPairRDD<String, String> cities = sc.parallelizePairs(Arrays.asList(
            new Tuple2<String, String>("Tokyo", "Japan"),
            new Tuple2<String, String>("San francisco", "America"),
            new Tuple2<String, String>("Beijing", "China")
        ));

        System.out.println("### Join ###");
        List<Tuple2<String, Tuple2<String, String>>> personsWithCountry1 = persons.mapToPair(
            new PairFunction<Tuple2<String, String>, String, String>() {
                public Tuple2<String, String> call(Tuple2<String, String> p) throws Exception {
                    return new Tuple2<String, String>(p._2(), p._1());
                }
            }).join(cities).collect();

        for (Tuple2<String, Tuple2<String, String>> p : personsWithCountry1) {
            System.out.println(String.format(
                "%-10s %-10s %-10s", p._2()._1(), p._2()._2(), p._1()));
        }
        System.out.println();

        System.out.println("### Left outer join ###");
        List<Tuple2<String, Tuple2<String, Optional<String>>>> personsWithCountry2 = persons.mapToPair(
            new PairFunction<Tuple2<String, String>, String, String>() {
                public Tuple2<String, String> call(Tuple2<String, String> p) throws Exception {
                    return new Tuple2<String, String>(p._2(), p._1());
                }
            }).leftOuterJoin(cities).collect();

        for (Tuple2<String, Tuple2<String, Optional<String>>> p : personsWithCountry2) {
            System.out.println(String.format(
                "%-10s %-10s %-10s", p._2()._1(), p._2()._2().or(""), p._1()));
        }
        System.out.println();

        System.out.println("### Right outer join ###");
        List<Tuple2<String, Tuple2<Optional<String>, String>>> personsWithCountry3 = persons.mapToPair(
            new PairFunction<Tuple2<String, String>, String, String>() {
                public Tuple2<String, String> call(Tuple2<String, String> p) throws Exception {
                    return new Tuple2<String, String>(p._2(), p._1());
                }
            }).rightOuterJoin(cities).collect();

        for (Tuple2<String, Tuple2<Optional<String>, String>> p : personsWithCountry3) {
            System.out.println(String.format(
                "%-10s %-10s %-10s", p._2()._1().or(""), p._2()._2(), p._1()));
        }
        System.out.println();

        System.out.println("### Full outer join ###");
        List<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> personsWithCountry4 = persons.mapToPair(
            new PairFunction<Tuple2<String, String>, String, String>() {
                public Tuple2<String, String> call(Tuple2<String, String> p) throws Exception {
                    return new Tuple2<String, String>(p._2(), p._1());
                }
            }).fullOuterJoin(cities).collect();

        for (Tuple2<String, Tuple2<Optional<String>, Optional<String>>> p : personsWithCountry4) {
            System.out.println(String.format(
                "%-10s %-10s %-10s", p._2()._1().or(""), p._2()._2().or(""), p._1()));
        }
        System.out.println();

        System.out.println("### Grouping ###");
        List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> grouped = persons.mapToPair(
            new PairFunction<Tuple2<String, String>, String, String>() {
                public Tuple2<String, String> call(Tuple2<String, String> p) throws Exception {
                    return new Tuple2<String, String>(p._2(), p._1());
                }
            }).cogroup(cities).collect();

        for (Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> p : grouped) {
            System.out.println(String.format(
                "%-20s %-20s %-20s", p._1(), p._2()._1(), p._2()._2()));
        }

        sc.stop();
    }
}
