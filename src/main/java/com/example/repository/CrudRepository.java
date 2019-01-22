package com.example.repository;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class CrudRepository implements HBaseOperations {

  static final int INCREMENT_COUNT = 1;

  @Autowired
  private Connection connection;

  @Override
  public Result get(String tableName, String rowName) throws IOException {
    return get(tableName, rowName, null, null);
  }

  @Override
  public Result get(String tableName, String rowName, String familyName) throws IOException {
    return get(tableName, rowName, familyName, null);
  }

  @Override
  public Result get(
    String tableName,
    String rowName,
    String familyName,
    String qualifier) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(rowName.getBytes());
    if (familyName != null) {
      byte[] family = familyName.getBytes();

      if (qualifier != null) {
        get.addColumn(family, qualifier.getBytes());
      } else {
        get.addFamily(family);
      }
    }

    Result result = table.get(get);
    table.close();
    return result;
  }

  @Override
  public Result[] batchGet(String tableName, List<Get> getList) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Result[] result = table.get(getList);
    table.close();
    return result;
  }

  public void batchGet(String tableName, List<Get> getList, Result[] results)
    throws IOException, InterruptedException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    table.batch(getList, results);
    table.close();
  }

  @Override
  public Put put(String tableName, String rowName, String familyName, String qualifier, byte[] data)
    throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Put put = new Put(rowName.getBytes());
    put.addColumn(familyName.getBytes(), qualifier.getBytes(), data);
    table.put(put);
    return put;
    //    table.close();
  }
  
  @Override
  public Put put(String tableName, byte[] rowName, String familyName, String qualifier, byte[] data)
    throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Put put = new Put(rowName);
    put.addColumn(familyName.getBytes(), qualifier.getBytes(), data);
    table.put(put);
    return put;
    //    table.close();
  }

  @Override
  public void batchPut(String tableName, List<Put> putList) throws Exception {
    Table table = connection.getTable(TableName.valueOf(tableName));
    table.put(putList);
    table.close();
  }

  @Override
  public void increment(
    String tableName,
    String rowName,
    String familyName,
    String qualifier,
    int amount)
    throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Increment increment = new Increment(Bytes.toBytes(rowName));
    increment.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), amount);
    table.increment(increment);
    table.close();
  }

  @Override
  public void batchIncrement(
    String tableName,
    List<Increment> increments) throws IOException, InterruptedException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Result[] results = new Result[increments.size()];
    table.batch(increments, results);
    table.close();
  }

  @Override
  public boolean exists(
    String tableName,
    String rowName,
    String familyName,
    String qualifier) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowName));
    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
    return table.exists(get);
  }

  public HashMap<String, BigDecimal> getColumnFamilyForCounter(
    String tableName, String rowKey, String columnFamily,
    Filter filter) throws IOException {
    HashMap<String, BigDecimal> hashMap = new HashMap<>();
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addFamily(Bytes.toBytes(columnFamily));

    if (filter != null) {
      get.setFilter(filter);
    }

    Result result = table.get(get);
    if (!result.isEmpty()) {
      result.listCells().forEach(cell -> {
        if (cell != null) {
          hashMap.put(
            new String(CellUtil.cloneQualifier(cell)),
            Bytes.toBigDecimal(CellUtil.cloneValue(cell)));
        }
      });
    }
    return hashMap;
  }

  @Override
  public void truncateTable(String tableName) throws IOException {
    Admin admin = connection.getAdmin();
    TableName table = TableName.valueOf(tableName);
    admin.disableTable(table);
    admin.truncateTable(table, false);
  }

  @Override
  public void dropTable(String tableName) throws IOException {
    Admin admin = connection.getAdmin();
    TableName table = TableName.valueOf(tableName);
    admin.disableTable(table);
    admin.deleteTable(table);
  }

}
