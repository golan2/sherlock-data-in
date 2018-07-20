package sherlok.datain;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.EsSpark;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DirectStream {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-consumer-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topic = Collections.singleton("data-in-test");

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("DirectStream");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(7));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topic, kafkaParams)
                );

        final String resource = "filebeat-1963.01.02/_doc";
        Map<String, String> result = new HashMap<>();
        result.put(ConfigurationOptions.ES_NODES , "localhost");
        result.put(ConfigurationOptions.ES_PORT , "9200");
        stream
                .map(cr -> cr.value())
//                .mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .foreachRDD( rdd -> {
                    EsSpark.saveToEs(rdd.rdd(), resource, JavaConversions.mapAsScalaMap(result));
                    System.out.println("rdd count = " + rdd.count());
                });
//                .foreachRDD( rdd -> rdd.saveAsTextFile("c:\\tmp\\streaming\\"+ new SimpleDateFormat("HH_mm_ss").format(new Date(System.currentTimeMillis())) +".txt"));

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
