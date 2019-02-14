package com.example;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HBaseConfiguration {

	private final Logger log = LoggerFactory.getLogger(HBaseConfiguration.class);

	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

	public HBaseConfiguration() {
	}

	@PostConstruct
	@Bean
	public Connection setupConnection() throws IOException {
		log.info("Setting up HBase connection");
		org.apache.hadoop.conf.Configuration configuration = org.apache.hadoop.hbase.HBaseConfiguration.create();
		configuration.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
		configuration.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENT_PORT, "2181");
		return ConnectionFactory.createConnection(configuration);
	}

}