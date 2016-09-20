package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGVCFRoundTrip implements Serializable {

  private static HBaseTestingUtility testUtil;

  private int splitSize = 1000;
  private TableName tableName = TableName.valueOf("gvcf");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    byte[][] columnFamilies = new byte[][]{GVCFHBase.SAMPLE_COLUMN_FAMILY};
    byte[][] splitKeys = new byte[][] {
        RowKey.getSplitKeyBytes("20", splitSize + 1)};
    HTable table = testUtil.createTable(tableName, columnFamilies, splitKeys);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testRoundTrip() throws Exception {
    List<VariantContext> expectedVariants = new ArrayList<>();
    VCFFileReader vcfFileReader = new VCFFileReader(new File("src/test/resources/g.vcf"), false);
    Iterators.addAll(expectedVariants, vcfFileReader.iterator());
    VCFHeader vcfHeader = vcfFileReader.getFileHeader();

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    JavaRDD<VariantContext> rdd = jsc.parallelize(expectedVariants);

    // insert into HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    SampleNameIndex sampleNameIndex = new SampleNameIndex(
        Lists.newArrayList(vcfHeader.getSampleNameToOffset().keySet())
    );
    HBaseVariantEncoder<VariantContext> variantEncoder =
        new HBaseVariantContextEncoder(sampleNameIndex, vcfHeader);
    GVCFHBase.put(rdd, variantEncoder, tableName, hbaseContext, splitSize);

    // scan
    List<VariantContext> actualVariants = GVCFHBase.scanSingle(variantEncoder, tableName,
        hbaseContext, "NA12878", sampleNameIndex)
        .collect();

    assertEqualVariants(expectedVariants, actualVariants);
  }

  /**
   * Validates that the given lists have variant
   * context that correspond to the same variants in the same order.
   * Compares VariantContext by comparing toStringDecodeGenotypes
   */
  public static void assertEqualVariants(final List<VariantContext> v1, final List<VariantContext> v2) {
    assertEquals(v1.size(), v2.size());
    for (int i = 0; i < v1.size(); i++) {
      assertEquals (v1.get(i).toStringDecodeGenotypes().replaceFirst("\\[VC [^ ]+", "[VC "),
          v2.get(i).toStringDecodeGenotypes().replaceFirst("\\[VC [^ ]+", "[VC "));
    }
  }

}
