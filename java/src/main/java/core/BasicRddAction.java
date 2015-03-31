package core;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class BasicRddAction {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Basic Rdd Action");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 1, 1, 2, 3, 3, 4, 4, 4, 5, 5, 5));
        JavaRDD<Str> strs = sc.parallelize(Arrays.asList(new Str("a"), new Str("b"), new Str("c"), new Str("d"), new Str("e")));

        System.out.println("### first ###");
        System.out.println(nums.first());
        System.out.println();

        System.out.println("### collect ###");
        System.out.println(nums.collect());
        System.out.println();

        System.out.println("### count ###");
        System.out.println(nums.count());
        System.out.println();

        System.out.println("### countByValue ###");
        for (Map.Entry<Integer, Long> e : new TreeMap<Integer, Long>(nums.countByValue()).entrySet()) {
            System.out.println(String.format("%d: %d", e.getKey(), e.getValue()));
        }
        System.out.println();

        System.out.println("### take ###");
        System.out.println(nums.take(5));
        System.out.println();

        System.out.println("### top ###");
        System.out.println(nums.top(4));
        System.out.println(strs.top(4, new StrComparator()));
        System.out.println();

        System.out.println("### takeOrdered ###");
        System.out.println(nums.takeOrdered(5));
        System.out.println(strs.takeOrdered(5, new StrComparator()));
        System.out.println();

        System.out.println("### takeSample ###");
        System.out.println(nums.takeSample(false, 3));
        System.out.println();

        System.out.println("### reduce ###");
        System.out.println(nums.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        }));
        System.out.println();

        System.out.println("### fold ###");
        System.out.println(nums.fold(0, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        }));
        System.out.println();

        System.out.println("### aggregate ###");
        System.out.println(nums.aggregate(
            0,
            new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer x, Integer y) throws Exception {
                    return x + y;
                }
            },
            new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer x, Integer y) throws Exception {
                    return x + y;
                }
            }
        ));
        System.out.println();

        System.out.println("### foreach ###");
        nums.foreach(new VoidFunction<Integer>() {
            public void call(Integer n) throws Exception {
                System.out.println(n);
            }
        });
        System.out.println();

        System.out.println("### foreachPartition ###");
        nums.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> it) throws Exception {
                List<Integer> l = new ArrayList<Integer>();
                while (it.hasNext()) {
                    l.add(it.next());
                }
                System.out.println(l);
            }
        });
        System.out.println();

        sc.stop();
    }

    public static class Str implements Serializable {
        private String s;

        public String getS() {
            return s;
        }

        public void setS(String s) {
            this.s = s;
        }

        public Str(String s) {
            this.s = s;
        }

        @Override
        public String toString() {
            return s;
        }
    }

    public static class StrComparator implements Comparator<Str>, Serializable {
        @Override
        public int compare(Str s1, Str s2) {
            return s1.getS().compareTo(s2.getS());
        }
    }
}
