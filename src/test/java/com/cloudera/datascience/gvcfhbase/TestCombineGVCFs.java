package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.Iterators;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.IOException;
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

public class TestCombineGVCFs {
  private static HBaseTestingUtility testUtil;

  private int splitSize = 4;
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
  public void testCombineGVCFs() throws Exception {
    List<VariantContext> variants1 = new ArrayList<>();
    VCFFileReader vcfFileReader1 = new VCFFileReader(new File("src/test/resources/g.vcf"), false);

    Iterators.addAll(variants1, vcfFileReader1.iterator());
    VCFHeader vcfHeader1 = vcfFileReader1.getFileHeader();

    List<VariantContext> variants2 = new ArrayList<>();
    VCFFileReader vcfFileReader2 = new VCFFileReader(new File("src/test/resources/g2.vcf"), false);
    Iterators.addAll(variants2, vcfFileReader2.iterator());
    VCFHeader vcfHeader2 = vcfFileReader2.getFileHeader();

    List<VariantContext> expectedAllVariants = new ArrayList<>();
    VCFFileReader expectedAllVariantsReader = new VCFFileReader(new File("src/test/resources/g1g2.vcf"), false);
    Iterators.addAll(expectedAllVariants, expectedAllVariantsReader.iterator());
    VCFHeader expectedAllVariantsHeader = expectedAllVariantsReader.getFileHeader();

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    JavaRDD<VariantContext> rdd1 = jsc.parallelize(variants1);
    JavaRDD<VariantContext> rdd2 = jsc.parallelize(variants2);

    // store in HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    SampleNameIndex sampleNameIndex = new SampleNameIndex(
        com.google.common.collect.Lists.newArrayList("NA12878", "NA12879")
    );
    HBaseVariantEncoder<VariantContext> variantEncoder1 =
        new HBaseVariantContextEncoder(sampleNameIndex, vcfHeader1);
    HBaseVariantEncoder<VariantContext> variantEncoder2 =
        new HBaseVariantContextEncoder(sampleNameIndex, vcfHeader2);
    GVCFHBase.store(rdd1, variantEncoder1, tableName, hbaseContext, splitSize);
    GVCFHBase.store(rdd2, variantEncoder2, tableName, hbaseContext, splitSize);

    List<VariantContext> allVariants = GVCFHBase.load(variantEncoder1, tableName,
        hbaseContext, new CombineGCVFsVariantCombiner()).collect();
    TestGVCF.assertEqualVariants(expectedAllVariants, allVariants);
  }
}
