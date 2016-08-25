package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import static org.junit.Assert.assertEquals;

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
    final byte[] sampleColumnFamily = Bytes.toBytes("s");
    HTable table = testUtil.createTable(tableName, sampleColumnFamily);

    for (TableName n : testUtil.getHBaseAdmin().listTableNames()) {
      System.out.println("tw: " + n.getNameAsString());
    }

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    ImmutableList<VariantLite> gvcf1 = ImmutableList.of(
        new VariantLite(1, 1, new GenotypeLite("a", "0|0")),
        new VariantLite(2, 7, new GenotypeLite("a", "0|1")),
        new VariantLite(8, 8, new GenotypeLite("a", "1|1")),
        new VariantLite(9, 12, new GenotypeLite("a", "0|0")),
        new VariantLite(13, 15, new GenotypeLite("a", "0|1")));

    ImmutableList<VariantLite> gvcf2 = ImmutableList.of(
        new VariantLite(1, 3, new GenotypeLite("b", "0|1")),
        new VariantLite(4, 6, new GenotypeLite("b", "1|1")),
        new VariantLite(7, 9, new GenotypeLite("b", "0|0")),
        new VariantLite(10, 15, new GenotypeLite("b", "0|1")));

    JavaRDD<VariantLite> rdd = jsc.parallelize(gvcf1);

    // insert into HBase
    // TODO: can we use bulkLoad for efficiency (need to port interface to Java)
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    hbaseContext.bulkPut(rdd, tableName, (Function<VariantLite, Put>) v -> {
      Put put = new Put(Bytes.toBytes(v.getStart()));
      byte[] qualifier = Bytes.toBytes(v.getGenotype().getSampleName());
      byte[] value = Bytes.toBytes(v.getGenotype().getValue());
      put.addColumn(sampleColumnFamily, qualifier, value);
      return put;
    });

    // TODO: figure out how to scan over the data properly
    // try using raw version of hbaseRDD, then call mapPartitions on it - this will
    // allow us to keep a list of VariantContexts (one for each sample) as we iterate
    // over the range
    // Read back into an RDD
    Scan scan = new Scan();
    scan.setCaching(100);
    JavaRDD<VariantLite> results = hbaseContext.hbaseRDD(tableName, scan,
        (Function<Tuple2<ImmutableBytesWritable, Result>, VariantLite>) t -> {
          Result result = t._2();
          int start = Bytes.toInt(result.getRow());
          List<GenotypeLite> genotypes = result.listCells().stream().map(cell -> {
            String qualifier = Bytes.toString(cell.getQualifierArray(), cell
                    .getQualifierOffset(),
                cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                cell.getValueLength());
            return new GenotypeLite(qualifier, value);
          }).collect(Collectors.toList());
          return new VariantLite(start, start, genotypes);
    });


    List<VariantLite> actualVariants = results.collect();
    assertEquals(gvcf1, actualVariants);

    testUtil.deleteTable(tableName);
  }
}
