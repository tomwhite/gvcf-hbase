package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.example.hbasecontext.JavaHBaseBulkPutExample;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
      System.out.println("tw: " + n.getNameAsString());
    }

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    List<String> list = ImmutableList.of("1,s,a,0|0", "2,s,a,1|1");
    JavaRDD<String> rdd = jsc.parallelize(list);

    // bulk insert into HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    hbaseContext.bulkPut(rdd, tableName, (Function<String, Put>) v -> {
      String[] cells = v.split(",");
      Put put = new Put(Bytes.toBytes(cells[0]));
      put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
          Bytes.toBytes(cells[3]));
      return put;
    });

    // TODO: read back data

    testUtil.deleteTable(tableName);
  }
}
