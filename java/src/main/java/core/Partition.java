package core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import scala.Tuple2;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Partition {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Partition");
        JavaSparkContext sc = new JavaSparkContext(conf);

        @SuppressWarnings("unchecked")
        JavaPairRDD<Integer, String> nums1 = sc.parallelizePairs(Arrays.asList(
            new Tuple2<Integer, String>(1, "a"),
            new Tuple2<Integer, String>(2, "b"),
            new Tuple2<Integer, String>(3, "c"),
            new Tuple2<Integer, String>(4, "d"),
            new Tuple2<Integer, String>(5, "e")
        ));
        JavaPairRDD<Integer, String> nums2 = nums1.partitionBy(new HashPartitioner(2));
        JavaPairRDD<Integer, String> nums3 = nums1.partitionBy(new RandomPartitioner(2));

        class PrintNums implements VoidFunction<Iterator<Tuple2<Integer, String>>> {
            public void call(Iterator<Tuple2<Integer, String>> it) throws Exception {
                while (it.hasNext()) {
                    Tuple2<Integer, String> e = it.next();
                    System.out.print(String.format("(%d -> %s) ", e._1(), e._2()));
                }
                System.out.println();
            }
        }

        System.out.println("### Default ###");
        nums1.foreachPartition(new PrintNums());
        System.out.println();

        System.out.println("### HashPartitioner ###");
        nums2.foreachPartition(new PrintNums());
        System.out.println();

        System.out.println("### RandomPartitioner ###");
        nums3.foreachPartition(new PrintNums());
        System.out.println();
    }

    public static class RandomPartitioner extends Partitioner {
        private int numParts;
        private Random rnd = new Random();

        public RandomPartitioner(int numParts) {
            this.numParts = numParts;
        }

        public int numPartitions() {
            return numParts;
        }

        public int getPartition(Object key) {
            return rnd.nextInt(Integer.MAX_VALUE) % numPartitions();
        }

        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof RandomPartitioner)) {
                return false;
            }
            return ((RandomPartitioner) other).numPartitions() == numPartitions();
        }
    }
}
