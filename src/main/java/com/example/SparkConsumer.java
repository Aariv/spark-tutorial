/**
 * 
 */
package com.example;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.example.dto.Cnr;
import com.example.dto.Cnr.CnrProtoMsg;

/**
 * @author kloudone
 *
 */
public class SparkConsumer {

	// Kafka brokers URL for Spark Streaming to connect and fetched messages from.
	private static final String KAFKA_BROKER_LIST = "localhost:9092";
	// Time interval in milliseconds of Spark Streaming Job, 10 seconds by default.
	private static final int STREAM_WINDOW_MILLISECONDS = 10000; // 10 seconds
	// Kafka telemetry topic to subscribe to. This should match to the topic in the
	// rule action.
	private static final Collection<String> TOPICS = Arrays.asList("bid-meta-5");
	// The application name
	public static final String APP_NAME = "Kafka Spark Streaming App For DMP";

	// Misc Kafka client properties
	private static Map<String, Object> getKafkaParams() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", DeserializerConfiguration.class);
		kafkaParams.put("group.id", "groupId-11");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("max.poll.records", 100);
		return kafkaParams;
	}

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local");

		try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS))) {
			
			JavaInputDStream<ConsumerRecord<String, Cnr.CnrProtoMsg>> stream = KafkaUtils.createDirectStream(ssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, Cnr.CnrProtoMsg>Subscribe(TOPICS, getKafkaParams()));

			stream.foreachRDD(rdd -> {
				// Map incoming JSON to cnrProtoMsg objects
				@SuppressWarnings("serial")
				JavaRDD<Cnr.CnrProtoMsg> cnrProtoMsg = rdd.map(new Function<ConsumerRecord<String,CnrProtoMsg>, CnrProtoMsg>() {

					@Override
					public CnrProtoMsg call(ConsumerRecord<String, CnrProtoMsg> v1) throws Exception {
						return v1.value();
					}
				});
				for (CnrProtoMsg proto : cnrProtoMsg.collect()) {
					System.out.println("Domain: " + proto.getBidMetaMsg().getAppInfo().getAppDomain());
				}
			});
			ssc.start();
			ssc.awaitTermination();
		}

	}
}
