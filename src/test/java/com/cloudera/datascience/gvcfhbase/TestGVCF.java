package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGVCF implements Serializable {

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

  private static String process(RowKey rowKey, Iterable<VariantLite> variants) {
    StringBuilder sb = new StringBuilder();
    sb.append(rowKey.contig).append(":").append(rowKey.pos).append(",");
    for (VariantLite variant : variants) {
      GenotypeLite genotype = variant.getGenotype();
      sb.append(genotype.getValue());
      sb.append("(end=").append(variant.getEnd()).append(")");
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  @Test
  public void test() throws Exception {
    int splitSize = 4;

    TableName tableName = TableName.valueOf("gvcf");
    byte[][] columnFamilies = new byte[][]{GVCFHBase.SAMPLE_COLUMN_FAMILY};
    byte[][] splitKeys = new byte[][]{Bytes.toBytes(splitSize + 1)};
    HTable table = testUtil.createTable(tableName, columnFamilies, splitKeys);

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    HBaseVariantEncoder<VariantLite> variantEncoder =
        new HBaseVariantLiteEncoder();

    ImmutableList<VariantLite> gvcf1 = ImmutableList.of(
        new VariantLite("20", 1, 1, new GenotypeLite(0, "0|0")),
        new VariantLite("20", 2, 7, new GenotypeLite(0, "N/A")),
        new VariantLite("20", 8, 8, new GenotypeLite(0, "1|1")));

    ImmutableList<VariantLite> gvcf2 = ImmutableList.of(
        new VariantLite("20", 1, 3, new GenotypeLite(1, "0|1")),
        new VariantLite("20", 4, 6, new GenotypeLite(1, "0|0")),
        new VariantLite("20", 7, 8, new GenotypeLite(1, "N/A")));

    JavaRDD<VariantLite> rdd1 = jsc.parallelize(gvcf1);
    JavaRDD<VariantLite> rdd2 = jsc.parallelize(gvcf2);

    // insert into HBase

    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    GVCFHBase.put(rdd1, variantEncoder, tableName, hbaseContext, splitSize);
    GVCFHBase.put(rdd2, variantEncoder, tableName, hbaseContext, splitSize);

    // Scan over all positions
    List<String> allPositions = GVCFHBase.scan(variantEncoder, tableName, hbaseContext,
        true, TestGVCF::process)
        .collect();
    //allPositions.forEach(System.out::println);
    List<String> expectedAllPositions = ImmutableList.of(
        "20:1,0|0(end=1),0|1(end=3)",
        "20:2,N/A(end=7),0|1(end=3)",
        "20:3,N/A(end=7),0|1(end=3)",
        "20:4,N/A(end=7),0|0(end=6)",
        "20:5,N/A(end=7),0|0(end=6)",
        "20:6,N/A(end=7),0|0(end=6)",
        "20:7,N/A(end=7),N/A(end=8)",
        "20:8,1|1(end=8),N/A(end=8)");
    assertEquals(expectedAllPositions, allPositions);

    // Scan over variants only
    List<String> allVariants = GVCFHBase.scan(variantEncoder, tableName, hbaseContext,
        false, TestGVCF::process)
        .collect();
    //allVariants.forEach(System.out::println);
    List<String> expectedAllVariants = ImmutableList.of(
        "20:1,0|0(end=1),0|1(end=3)",
        "20:4,N/A(end=7),0|0(end=6)",
        "20:8,1|1(end=8),N/A(end=8)");
    assertEquals(expectedAllVariants, allVariants);

    testUtil.deleteTable(tableName);
  }

}
