package com.cloudera.datascience.gvcfhbase;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
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

public class TestGVCF implements Serializable {

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

  private static String process(RowKey rowKey, Iterable<VariantContext> variants) {
    StringBuilder sb = new StringBuilder();
    sb.append(rowKey.contig).append(":").append(rowKey.pos).append(",");
    for (VariantContext variant : variants) {
      List<Allele> alleles = variant.getAlleles();
      Genotype genotype = variant.getGenotype(0);
      sb.append(alleles.get(0).getDisplayString()).append(":");
      sb.append(alleles.get(1).getDisplayString()).append(":");
      sb.append(genotypeToString(genotype, alleles));
      sb.append("(end=").append(variant.getEnd()).append(")");
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private static String genotypeToString(Genotype genotype, List<Allele> alleles) {
    StringBuilder sb = new StringBuilder();
    for (Allele a : genotype.getAlleles()) {
      int index = Iterables.indexOf(alleles, a::equals);
      sb.append(index).append(genotype.isPhased() ? "|" : "/");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  @Test
  public void testMatchingBlocks() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "a", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "a", "1/1"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "b", "1/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "b", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "b", "0/1"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1,A:G:0/1(end=1),A:G:1/1(end=1)",
        "20:2,G:<NON_REF>:0/0(end=4),G:<NON_REF>:0/0(end=4)",
        "20:5,G:<NON_REF>:0/0(end=7),G:<NON_REF>:0/0(end=7)", // due to split
        "20:8,G:C:1/1(end=8),G:C:0/1(end=8)");

    check(gvcf1, gvcf2, expectedAllVariants);
  }

  @Test
  public void testSNPAndNonRef() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "<NON_REF>", "b", "0/0"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1,A:G:0/1(end=1),A:<NON_REF>:0/0(end=1)");

    check(gvcf1, gvcf2, expectedAllVariants);
  }

  @Test
  public void testSNPAndNonRefBlock() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"),
        newVariantContext("20", 2, 2, "G", "<NON_REF>", "a", "0/0"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 2, "A", "<NON_REF>", "b", "0/0"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1,A:G:0/1(end=1),A:<NON_REF>:0/0(end=1)",
        "20:2,G:<NON_REF>:0/0(end=2),A:<NON_REF>:0/0(end=2)"); // A is wrong ref here

    check(gvcf1, gvcf2, expectedAllVariants);
  }

  private static VariantContext newVariantContext(String contig, int start, int end,
      String ref, String alt, String sampleName, String genotypeString) {
    VariantContextBuilder builder = new VariantContextBuilder();
    builder.source(TestGVCF.class.getSimpleName());
    builder.chr(contig);
    builder.start(start);
    builder.stop(end);
    builder.alleles(ref, alt);

    final List<Allele> alleles = builder.getAlleles();
    List<String> genotypeAlleleIndexes = Lists.newArrayList(
        Splitter.onPattern("\\||/").split(genotypeString));
    List<Allele> genotypeAlleles = genotypeAlleleIndexes.stream()
        .map(s -> alleles.get(Integer.parseInt(s)))
        .collect(Collectors.toList());
    builder.genotypes(new GenotypeBuilder(sampleName, genotypeAlleles).make());
    return builder.make();
  }

  private void check(List<VariantContext> gvcf1, List<VariantContext> gvcf2,
      List<String> expectedAllVariants) throws Exception {

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    JavaRDD<VariantContext> rdd1 = jsc.parallelize(gvcf1);
    JavaRDD<VariantContext> rdd2 = jsc.parallelize(gvcf2);

    // insert into HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    SampleNameIndex sampleNameIndex = new SampleNameIndex(ImmutableList.of("a", "b"));
    HBaseVariantEncoder<VariantContext> variantEncoder =
        new HBaseVariantContextEncoder(sampleNameIndex);
    GVCFHBase.put(rdd1, variantEncoder, tableName, hbaseContext, splitSize);
    GVCFHBase.put(rdd2, variantEncoder, tableName, hbaseContext, splitSize);

    // Scan over variants
    List<String> allVariants = GVCFHBase.scan(variantEncoder, tableName, hbaseContext,
        TestGVCF::process)
        .collect();
    //allVariants.forEach(System.out::println);
    assertEquals(expectedAllVariants, allVariants);

    testUtil.deleteTable(tableName);
  }

}
