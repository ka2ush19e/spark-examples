package core;

import java.util.Arrays;

import scala.Tuple2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class FileLoadSave {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("File Load Save");
        JavaSparkContext sc = new JavaSparkContext(conf);

        @SuppressWarnings("unchecked")
        JavaPairRDD<Integer, String> saveData = sc.parallelizePairs(Arrays.asList(
            new Tuple2<Integer, String>(1, "a"),
            new Tuple2<Integer, String>(1, "b"),
            new Tuple2<Integer, String>(1, "c"),
            new Tuple2<Integer, String>(2, "d"),
            new Tuple2<Integer, String>(2, "e")
        ));

        System.out.println("### SequenceFile ###");
        saveData.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
            public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<IntWritable, Text>(new IntWritable(t._1()), new Text(t._2()));
            }
        }).saveAsHadoopFile(
            "output_sequence", IntWritable.class, Text.class, SequenceFileOutputFormat.class);

        JavaPairRDD<Integer, String> loadDataFromSequenceFile =
            sc.sequenceFile("output_sequence", IntWritable.class, Text.class).mapToPair(
                new PairFunction<Tuple2<IntWritable, Text>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<IntWritable, Text> t) throws Exception {
                        return new Tuple2<Integer, String>(t._1().get(), t._2().toString());
                    }
                }
            );

        for (Tuple2<Integer, String> e : loadDataFromSequenceFile.collect()) {
            System.out.println(String.format("%d: %s", e._1(), e._2()));
        }
        System.out.println();

        System.out.println("### ObjectFile ###");
        saveData.saveAsObjectFile("output_object");

        @SuppressWarnings("unchecked")
        JavaPairRDD<Integer, String> loadDataFromObjectFile =
            sc.objectFile("output_object").mapToPair(new PairFunction<Object, Integer, String>() {
                public Tuple2<Integer, String> call(Object o) throws Exception {
                    return (Tuple2<Integer, String>) o;
                }
            });
        for (Tuple2<Integer, String> e : loadDataFromObjectFile.collect()) {
            System.out.println(String.format("%d: %s", e._1(), e._2()));
        }
        System.out.println();

        System.out.println("### HadoopFile ###");
        saveData.saveAsHadoopFile("output_hadoop", IntWritable.class, Text.class, TextOutputFormat.class);

        JavaPairRDD<Integer, String> loadDataFromHadoopFile = sc.hadoopFile(
            "output_hadoop", KeyValueTextInputFormat.class, Text.class, Text.class).mapToPair(
            new PairFunction<Tuple2<Text, Text>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<Text, Text> t) throws Exception {
                    return new Tuple2<Integer, String>(
                        Integer.parseInt(t._1().toString()),
                        t._2().toString()
                    );
                }
            }
        );
        for (Tuple2<Integer, String> e : loadDataFromHadoopFile.collect()) {
            System.out.println(String.format("%s: %s", e._1(), e._2()));
        }
        System.out.println();

        System.out.println("### Compressed HadoopFile ###");
        saveData.saveAsHadoopFile(
            "output_hadoop_compressed", IntWritable.class, Text.class, TextOutputFormat.class, GzipCodec.class);

        JavaPairRDD<Integer, String> loadDataFromCompressedHadoopFile = sc.hadoopFile(
            "output_hadoop_compressed", KeyValueTextInputFormat.class, Text.class, Text.class).mapToPair(
            new PairFunction<Tuple2<Text, Text>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<Text, Text> t) throws Exception {
                    return new Tuple2<Integer, String>(
                        Integer.parseInt(t._1().toString()),
                        t._2().toString()
                    );
                }
            }
        );
        for (Tuple2<Integer, String> e : loadDataFromCompressedHadoopFile.collect()) {
            System.out.println(String.format("%s: %s", e._1(), e._2()));
        }
        sc.stop();
    }
}
