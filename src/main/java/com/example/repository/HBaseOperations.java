package com.example.repository;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;

public interface HBaseOperations {

	Result get(String tableName, String rowName) throws IOException;

	Result get(String tableName, String rowName, String familyName) throws IOException;

	Result get(String tableName, final String rowName, final String familyName, final String qualifier)
			throws IOException;

	Result[] batchGet(String tableName, List<Get> getList) throws IOException;

	Put put(String tableName, final String rowName, final String familyName, final String qualifier, final byte[] data)
			throws IOException;

	Put put(String tableName, final byte[] rowName, final String familyName, final String qualifier, final byte[] data)
			throws IOException;

	void batchPut(String tableName, List<Put> putList) throws Exception;

	void batchIncrement(String tableName, List<Increment> incrementList) throws IOException, InterruptedException;

	void increment(String tableName, String rowName, String familyName, String qualifier, int amount)
			throws IOException;

	HashMap<String, BigDecimal> getColumnFamilyForCounter(String tableName, String rowKey, String columnFamily,
			Filter filter) throws IOException;

	boolean exists(String tableName, String rowName, String familyName, String qualifier) throws IOException;

	public void truncateTable(String tableName) throws IOException;

	public void dropTable(String tableName) throws IOException;
}
