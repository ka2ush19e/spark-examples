package core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SetOperation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Set Operation");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> nums1 = sc.parallelize(Arrays.asList(5, 5, 4, 4, 3, 3, 2, 2, 1, 1));
        JavaRDD<Integer> nums2 = sc.parallelize(Arrays.asList(5, 3, 3, 1, 1));

        System.out.println("### Distinct ###");
        System.out.println(nums1.distinct().collect());
        System.out.println();

        System.out.println("### Union ###");
        System.out.println(nums1.union(nums2).collect());
        System.out.println();

        System.out.println("### Intersection ###");
        System.out.println(nums1.intersection(nums2).collect());
        System.out.println();

        System.out.println("### Subtract ###");
        System.out.println(nums1.subtract(nums2).collect());
        System.out.println();

        System.out.println("### Cartesian ###");
        System.out.println(nums1.cartesian(nums2).collect());
        System.out.println();
    }
}
