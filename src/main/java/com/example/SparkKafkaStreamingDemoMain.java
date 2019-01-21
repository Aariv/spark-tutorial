package com.example;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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

import com.example.dto.User;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkKafkaStreamingDemoMain {

	// Kafka brokers URL for Spark Streaming to connect and fetched messages from.
	private static final String KAFKA_BROKER_LIST = "localhost:9092";
	// Time interval in milliseconds of Spark Streaming Job, 10 seconds by default.
	private static final int STREAM_WINDOW_MILLISECONDS = 100000; // 10 seconds
	// Kafka telemetry topic to subscribe to. This should match to the topic in the
	// rule action.
	private static final Collection<String> TOPICS = Arrays.asList("votes");
	// The application name
	public static final String APP_NAME = "Kafka Spark Streaming App";

	// Misc Kafka client properties
	private static Map<String, Object> getKafkaParams() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "groupId-15");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		return kafkaParams;
	}

	public static void main(String[] args) throws Exception {
		new StreamRunner().start();
	}

	private static class StreamRunner {

		void start() throws Exception {
			SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[4]");

			try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS))) {

				JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(TOPICS, getKafkaParams()));

				stream.foreachRDD(rdd -> {
					// Map incoming JSON to User objects
					JavaRDD<User> userRDD = rdd.map(new UserDataMapper());
					//
					List<User> aggData = userRDD.map(t -> new User(t.getUsername(), t.getPassword())).collect();
					for (User user : aggData) {
						System.out.println("Username" + user.getUsername());
					}
					System.out.println(aggData.size());
				});

				ssc.start();
				ssc.awaitTermination();
			}
		}

		@SuppressWarnings("serial")
		private static class UserDataMapper implements Function<ConsumerRecord<String, String>, User> {
			private static final ObjectMapper mapper = new ObjectMapper();

			@Override
			public User call(ConsumerRecord<String, String> record) throws Exception {
				System.out.println(record);
				return mapper.readValue(record.value(), User.class);
			}
		}
	}
}