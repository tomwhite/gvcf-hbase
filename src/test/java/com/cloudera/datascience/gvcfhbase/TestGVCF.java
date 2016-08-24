package com.cloudera.datascience.gvcfhbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGVCF {

  private static HBaseTestingUtility testUtil;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    TableName tableName = TableName.valueOf("gvcf");
    HTable table = testUtil.createTable(tableName, Bytes.toBytes("s")); // s for sample

    for (TableName n : testUtil.getHBaseAdmin().listTableNames()) {
      System.out.println(n.getNameAsString());
    }

    testUtil.deleteTable(tableName);
  }
}
