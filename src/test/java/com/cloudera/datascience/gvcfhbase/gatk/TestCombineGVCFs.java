package com.cloudera.datascience.gvcfhbase.gatk;

import com.cloudera.datascience.gvcfhbase.GVCFHBase;
import com.cloudera.datascience.gvcfhbase.HBaseVariantContextEncoder;
import com.cloudera.datascience.gvcfhbase.HBaseVariantEncoder;
import com.cloudera.datascience.gvcfhbase.SampleNameIndex;
import com.cloudera.datascience.gvcfhbase.TestGVCF;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestCombineGVCFs {
  private static HBaseTestingUtility testUtil;

  private final List<String> singleSampleGvcfFiles;
  private final String expectedCombinedVcf;
  private final String referenceFile;

  private int splitSize = Integer.MAX_VALUE;
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
        GVCFHBase.getSplitKeyBytes("20", splitSize + 1)};
    HTable table = testUtil.createTable(tableName, columnFamilies, splitKeys);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
//        { ImmutableList.of("src/test/resources/g.vcf", "src/test/resources/g2.vcf"),
//            "src/test/resources/g1g2.vcf",
//            "/Users/tom/workspace/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta" },
//        { ImmutableList.of("src/test/resources/g.vcf", "src/test/resources/g3.vcf"),
//            "src/test/resources/g1g3.vcf",
//            "/Users/tom/workspace/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta" },
        // TODO: investigate failing test and fix
//        { ImmutableList.of("src/test/resources/g.vcf", "src/test/resources/g4.vcf"),
//            "src/test/resources/g1g4.vcf",
//            "/Users/tom/workspace/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta" },
        // TODO: investigate failing test and fix
//        { ImmutableList.of("src/test/resources/t0.vcf", "src/test/resources/t1.vcf", "src/test/resources/t2.vcf"),
//            "src/test/resources/t0t1t2.vcf", // use this combined vcf (produced by running CombineGVCFs) rather than t0_1_2_combined.vcf (from GenomicsDB)
//            "src/test/resources/chr1_10MB.fasta" },
    });
  }

  public TestCombineGVCFs(List<String> singleSampleGvcfFiles, String
      expectedCombinedVcf, String referenceFile) {
    this.singleSampleGvcfFiles = singleSampleGvcfFiles;
    this.expectedCombinedVcf = expectedCombinedVcf;
    this.referenceFile = referenceFile;
  }

  @Test
  public void testCombineGVCFs() throws Exception {
    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    List<String> sampleNames = new ArrayList<>();
    List<JavaRDD<VariantContext>> rdds = new ArrayList<>();
    List<VCFHeader> headers = new ArrayList<>();
    for (String file : singleSampleGvcfFiles) {
      List<VariantContext> variants = new ArrayList<>();
      VCFFileReader vcfFileReader = new VCFFileReader(new File(file), false);

      Iterators.addAll(variants, vcfFileReader.iterator());
      VCFHeader vcfHeader = vcfFileReader.getFileHeader();
      headers.add(vcfHeader);
      String sampleName = Iterables.getOnlyElement(vcfHeader.getSampleNamesInOrder());
      sampleNames.add(sampleName);

      JavaRDD<VariantContext> rdd = jsc.parallelize(variants);
      rdds.add(rdd);
    }

    // store in HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    SampleNameIndex sampleNameIndex = new SampleNameIndex(sampleNames);
    for (int i = 0; i < rdds.size(); i++) {
      JavaRDD<VariantContext> rdd = rdds.get(i);
      VCFHeader vcfHeader = headers.get(i);
      HBaseVariantEncoder<VariantContext> variantEncoder =
          new HBaseVariantContextEncoder(sampleNameIndex, vcfHeader);
      String sampleName = vcfHeader.getSampleNamesInOrder().get(0);
      GVCFHBase.store(rdd, variantEncoder, tableName, hbaseContext, splitSize, jsc,
          sampleName, referenceFile);
    }

    HBaseVariantEncoder<VariantContext> variantEncoder =
        new HBaseVariantContextEncoder(sampleNameIndex, headers.get(0));
    List<VariantContext> allVariants = CombineGCVFs.combine(variantEncoder, tableName, hbaseContext,
        referenceFile)
        .collect();

    List<VariantContext> expectedAllVariants = new ArrayList<>();
    VCFFileReader expectedAllVariantsReader = new VCFFileReader(new File(expectedCombinedVcf), false);
    Iterators.addAll(expectedAllVariants, expectedAllVariantsReader.iterator());

    TestGVCF.assertEqualVariants(expectedAllVariants, allVariants);

    jsc.stop();

    testUtil.deleteTable(tableName);
  }
}
