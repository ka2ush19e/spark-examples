package streaming;

import java.util.Arrays;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class NetworkWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Network Word Count");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]));
        JavaPairDStream<String, Long> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String l) throws Exception {
                return Arrays.asList(l.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String w) throws Exception {
                return new Tuple2<String, Long>(w, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
