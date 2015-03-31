package core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

public class GroupBy {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("GroupBy");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(0, 1, 2, 3, 4), 5)
            .flatMap(new FlatMapFunction<Integer, Integer>() {
                public Iterable<Integer> call(Integer n) throws Exception {
                    Random rnd = new Random();
                    List<Integer> l = new ArrayList<Integer>();
                    for (int i = 0; i < 1000; i++) {
                        l.add(rnd.nextInt(10));
                    }
                    return l;
                }
            })
            .persist(StorageLevel.MEMORY_AND_DISK_SER());

        System.out.println("### Count ###");
        System.out.println(nums.count());
        System.out.println();

        System.out.println("### Average: reduce ###");
        Tuple2<Integer, Integer> avg1 = nums
            .map(new Function<Integer, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .reduce(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                    return new Tuple2<Integer, Integer>(v1._1() + v2._1(), v1._2() + v2._2());
                }
            });
        System.out.println(avg1._1() * 1.0 / avg1._2());
        System.out.println();

        System.out.println("### Average: fold ###");
        Tuple2<Integer, Integer> avg2 = nums
            .map(new Function<Integer, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .fold(
                new Tuple2<Integer, Integer>(0, 0),
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<Integer, Integer>(v1._1() + v2._1(), v1._2() + v2._2());
                    }
                });
        System.out.println(avg2._1() * 1.0 / avg2._2());
        System.out.println();

        System.out.println("### Average: aggregate ###");
        Tuple2<Integer, Integer> avg3 = nums
            .map(new Function<Integer, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .aggregate(
                new Tuple2<Integer, Integer>(0, 0),
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<Integer, Integer>(v1._1() + v2._1(), v1._2() + v2._2());
                    }
                },
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<Integer, Integer>(v1._1() + v2._1(), v1._2() + v2._2());
                    }
                }
            );
        System.out.println(avg3._1() * 1.0 / avg3._2());
        System.out.println();

        System.out.println("### Grouping: reduceByKey ###");
        List<Tuple2<Integer, Integer>> grouped1 = nums
            .mapToPair(new PairFunction<Integer, Integer, Integer>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            })
            .sortByKey().collect();
        for (Tuple2<Integer, Integer> e : grouped1) {
            System.out.println(e._1() + " " + e._2());
        }
        System.out.println();

        System.out.println("### Grouping: foldByKey ###");
        List<Tuple2<Integer, Integer>> grouped2 = nums
            .mapToPair(new PairFunction<Integer, Integer, Integer>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .foldByKey(0, new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            })
            .sortByKey().collect();
        for (Tuple2<Integer, Integer> e : grouped2) {
            System.out.println(e._1() + " " + e._2());
        }
        System.out.println();

        System.out.println("### Grouping: aggregateByKey ###");
        List<Tuple2<Integer, Integer>> grouped3 = nums
            .mapToPair(new PairFunction<Integer, Integer, Integer>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .aggregateByKey(
                0,
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
            )
            .sortByKey().collect();
        for (Tuple2<Integer, Integer> e : grouped3) {
            System.out.println(e._1() + " " + e._2());
        }
        System.out.println();

        System.out.println("### Grouping: combineByKey ###");
        List<Tuple2<Integer, Integer>> grouped4 = nums
            .mapToPair(new PairFunction<Integer, Integer, Integer>() {
                public Tuple2<Integer, Integer> call(Integer n) throws Exception {
                    return new Tuple2<Integer, Integer>(n, 1);
                }
            })
            .combineByKey(
                new Function<Integer, Integer>() {
                    public Integer call(Integer v) throws Exception {
                        return v;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
            )
            .sortByKey().collect();
        for (Tuple2<Integer, Integer> e : grouped4) {
            System.out.println(e._1() + " " + e._2());
        }

        sc.stop();
    }
}
