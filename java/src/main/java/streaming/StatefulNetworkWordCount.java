package streaming;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StatefulNetworkWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Stateful Network Word Count");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
        ssc.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]));
        JavaPairDStream<String, Long> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String l) throws Exception {
                return Arrays.asList(l.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String w) throws Exception {
                return new Tuple2<String, Long>(w, 1L);
            }
        }).updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            public Optional<Long> call(List<Long> values, Optional<Long> state) throws Exception {
                if (values == null || values.isEmpty()) {
                    return state;
                }
                long sum = 0L;
                for (Long v : values) {
                    sum += v;
                }
                return Optional.of(state.or(0L) + sum);
            }
        });

        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
