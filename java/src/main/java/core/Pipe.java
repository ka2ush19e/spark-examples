package core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Pipe {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Pipe");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String columnCountScript = "./src/main/scripts/columncount.py";
        String columnCountScriptName = "columncount.py";
        sc.addFile(columnCountScript);

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("1,2,3", "4,5", "6", "7,8,9,10"));
        List<String> columnCounts = lines.pipe(SparkFiles.get(columnCountScriptName)).collect();

        System.out.println(columnCounts);

        sc.stop();
    }
}
