package com.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.dto.User;
import com.example.repository.CrudRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class SparkTutorialApplication implements CommandLineRunner {

	@Autowired
	private CrudRepository crudRepository;

	// Kafka brokers URL for Spark Streaming to connect and fetched messages from.
	private static final String KAFKA_BROKER_LIST = "localhost:9092";
	// Time interval in milliseconds of Spark Streaming Job, 10 seconds by default.
	private static final int STREAM_WINDOW_MILLISECONDS = 10000; // 10 seconds
	// Kafka telemetry topic to subscribe to. This should match to the topic in the
	// rule action.
	private static final Collection<String> TOPICS = Arrays.asList("users-02");
	// The application name
	public static final String APP_NAME = "Kafka Spark Streaming App";

	// Misc Kafka client properties
	private static Map<String, Object> getKafkaParams() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "groupId-01");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("max.poll.records", 100);
		kafkaParams.put("max.poll.interval.ms", 300000);
		return kafkaParams;
	}

	public static void main(String[] args) {
		SpringApplication.run(SparkTutorialApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local");

		try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS))) {

			JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(TOPICS, getKafkaParams()));

			stream.foreachRDD(rdd -> {
				// Map incoming JSON to User objects
				JavaRDD<User> userRDD = rdd.map(new UserDataMapper());
				//
				List<User> aggData = userRDD.map(t -> new User(t.getUsername(), t.getPassword())).collect();
				List<Put> putList = new ArrayList<>();
				for (User user : aggData) {
					String rowID = UUID.randomUUID().toString();
					putList.add(
							crudRepository.put("tbl_user", rowID, "users", "username", user.getUsername().getBytes()));
					putList.add(
							crudRepository.put("tbl_user", rowID, "users", "password", user.getPassword().getBytes()));
				}
				crudRepository.batchPut("tbl_user", putList);
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
