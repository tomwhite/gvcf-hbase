package com.cloudera.datascience.gvcfhbase;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
import static org.junit.Assert.assertNotEquals;

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

  private static class PrintVariantCombiner implements VariantCombiner<VariantContext, String> {
    @Override
    public Iterable<String> combine(Locatable loc, Iterable<VariantContext>
        variants) {
      StringBuilder sb = new StringBuilder();
      sb.append(loc.getContig()).append(":").append(loc.getStart()).append("-")
          .append(loc.getEnd()).append(",");
      for (VariantContext variant : variants) {
        if (variant == null) {
          sb.append("./.,");
          continue;
        }
        List<Allele> alleles = variant.getAlleles();
        Genotype genotype = variant.getGenotype(0);
        sb.append(alleles.get(0).getDisplayString()).append(":");
        sb.append(alleles.get(1).getDisplayString()).append(":");
        sb.append(genotypeToString(genotype, alleles));
        sb.append("(").append(variant.getStart()).append("-")
            .append(variant.getEnd()).append(")");
        sb.append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
      return ImmutableList.of(sb.toString());
    }

    @Override
    public Iterable<String> finish() {
      return ImmutableList.of();
    }
  }

  private static class SimpleMindedVariantCombiner implements VariantCombiner<VariantContext,
      VariantContext> {
    @Override
    public Iterable<VariantContext> combine(Locatable loc, Iterable<VariantContext> v) {
      List<VariantContext> variants = Lists.newArrayList(v);
      VariantContext firstVariant = variants.get(0);
      if (firstVariant.getStart() != loc.getStart()) { // ignore fake variant from split
        return ImmutableList.of();
      }
      VariantContextBuilder builder = new VariantContextBuilder();
      builder.source(TestGVCF.class.getSimpleName());
      builder.chr(firstVariant.getContig());
      builder.start(firstVariant.getStart());
      builder.stop(firstVariant.getEnd());
      builder.alleles(firstVariant.getAlleles());
      List<Genotype> genotypes = Lists.newArrayList(firstVariant.getGenotypes());
      for (int i = 1; i < variants.size(); i++) {
        genotypes.addAll(variants.get(i).getGenotypes());
      }
      builder.genotypes(genotypes);
      return ImmutableList.of(builder.make());
    }

    @Override
    public Iterable<VariantContext> finish() {
      return ImmutableList.of();
    }
  }

  private static String genotypeToString(Genotype genotype, List<Allele> alleles) {
    StringBuilder sb = new StringBuilder();
    for (Allele a : genotype.getAlleles()) {
      int index = Iterables.indexOf(alleles, a::equals);
      assertNotEquals(-1, index);
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
        "20:1-1,A:G:0/1(1-1),A:G:1/1(1-1)",
        "20:2-4,G:<NON_REF>:0/0(2-7),G:<NON_REF>:0/0(2-7)",
        "20:5-7,G:<NON_REF>:0/0(2-7),G:<NON_REF>:0/0(2-7)", // due to split
        "20:8-8,G:C:1/1(8-8),G:C:0/1(8-8)");

    List<String> allVariants = storeAndLoad(gvcf1, gvcf2, new PrintVariantCombiner());
    assertEquals(expectedAllVariants, allVariants);
  }

  @Test
  public void testCombineMatchingBlocks() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "a", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "a", "1/1"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "b", "1/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "b", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "b", "0/1"));

    ImmutableList<VariantContext> combinedGvcf = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1", "b", "1/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "a", "0/0", "b", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "a", "1/1", "b", "0/1"));

    List<VariantContext> allVariants = storeAndLoad(gvcf1, gvcf2, new SimpleMindedVariantCombiner());
    assertEqualVariants(combinedGvcf, allVariants);
  }

  @Test
  public void testSNPAndNonRef() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "<NON_REF>", "b", "0/0"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1-1,A:G:0/1(1-1),A:<NON_REF>:0/0(1-1)");

    List<String> allVariants = storeAndLoad(gvcf1, gvcf2, new PrintVariantCombiner());
    assertEquals(expectedAllVariants, allVariants);

  }

  @Test
  public void testSNPAndNonRefBlock() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"),
        newVariantContext("20", 2, 2, "G", "<NON_REF>", "a", "0/0"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 2, "A", "<NON_REF>", "b", "0/0"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1-1,A:G:0/1(1-1),A:<NON_REF>:0/0(1-2)",
        "20:2-2,G:<NON_REF>:0/0(2-2),A:<NON_REF>:0/0(1-2)");

    List<String> allVariants = storeAndLoad(gvcf1, gvcf2, new PrintVariantCombiner());
    assertEquals(expectedAllVariants, allVariants);

  }

  @Test
  public void testNoCall() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "a", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "a", "1/1"));

    // note missing calls for positions 2-8
    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "b", "1/1"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1-1,A:G:0/1(1-1),A:G:1/1(1-1)",
        "20:2-4,G:<NON_REF>:0/0(2-7),./.",
        "20:5-7,G:<NON_REF>:0/0(2-7),./.", // due to split
        "20:8-8,G:C:1/1(8-8),./.");

    List<String> allVariants = storeAndLoad(gvcf1, gvcf2, new PrintVariantCombiner());
    assertEquals(expectedAllVariants, allVariants);
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
    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "b", "1/1"),
        newVariantContext("20", 2, 7, "G", "<NON_REF>", "b", "0/0"),
        newVariantContext("20", 8, 8, "G", "C", "b", "0/1"));
    JavaRDD<VariantContext> rdd2 = jsc.parallelize(gvcf2);

    // store in HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    SampleNameIndex sampleNameIndex = new SampleNameIndex(
        com.google.common.collect.Lists.newArrayList("NA12878", "b")
    );
    HBaseVariantEncoder<VariantContext> variantEncoder =
        new HBaseVariantContextEncoder(sampleNameIndex, vcfHeader);
    GVCFHBase.store(rdd, variantEncoder, tableName, hbaseContext, splitSize);
    GVCFHBase.store(rdd2, variantEncoder, tableName, hbaseContext, splitSize);

    // load from HBase
    List<VariantContext> actualVariants = GVCFHBase.loadSingleSample(variantEncoder, tableName,
        hbaseContext, "NA12878", sampleNameIndex)
        .collect();
    assertEqualVariants(expectedVariants, actualVariants);

    List<VariantContext> actualVariants2 = GVCFHBase.loadSingleSample(variantEncoder, tableName,
        hbaseContext, "b", sampleNameIndex)
        .collect();
    assertEqualVariants(gvcf2, actualVariants2);

    jsc.stop();

    testUtil.deleteTable(tableName);
  }

  private static VariantContext newVariantContext(String contig, int start, int end,
      String ref, String alt, String sampleName, String genotypeString) {
    VariantContextBuilder builder = new VariantContextBuilder();
    builder.source(TestGVCF.class.getSimpleName());
    builder.chr(contig);
    builder.start(start);
    builder.stop(end);
    builder.alleles(ref, alt);
    builder.genotypes(newGenotype(builder.getAlleles(), sampleName, genotypeString));
    return builder.make();
  }

  private static VariantContext newVariantContext(String contig, int start, int end,
      String ref, String alt, String sampleName1, String genotypeString1,
      String sampleName2, String genotypeString2) {
    VariantContextBuilder builder = new VariantContextBuilder();
    builder.source(TestGVCF.class.getSimpleName());
    builder.chr(contig);
    builder.start(start);
    builder.stop(end);
    builder.alleles(ref, alt);
    builder.genotypes(newGenotype(builder.getAlleles(), sampleName1, genotypeString1),
        newGenotype(builder.getAlleles(), sampleName2, genotypeString2));
    return builder.make();
  }

  private static Genotype newGenotype(List<Allele> alleles, String sampleName, String
      genotypeString) {
    List<String> genotypeAlleleIndexes = Lists.newArrayList(
        Splitter.onPattern("\\||/").split(genotypeString));
    List<Allele> genotypeAlleles = genotypeAlleleIndexes.stream()
        .map(s -> alleles.get(Integer.parseInt(s)))
        .collect(Collectors.toList());
    return new GenotypeBuilder(sampleName, genotypeAlleles).make();
  }

  private <T> List<T> storeAndLoad(List<VariantContext> gvcf1, List<VariantContext> gvcf2,
      VariantCombiner<VariantContext, T> variantCombiner) throws Exception {

    // create an RDD
    SparkConf sparkConf = new SparkConf()
        .setMaster("local")
        .setAppName(getClass().getSimpleName() + tableName)
        .set("spark.io.compression.codec", "lzf");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    JavaRDD<VariantContext> rdd1 = jsc.parallelize(gvcf1);
    JavaRDD<VariantContext> rdd2 = jsc.parallelize(gvcf2);

    // store in HBase
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    ImmutableList<String> sampleNames = ImmutableList.of("a", "b");
    SampleNameIndex sampleNameIndex = new SampleNameIndex(sampleNames);
    VCFHeader vcfHeader = new VCFHeader(Sets.newHashSet(), sampleNames);
    HBaseVariantEncoder<VariantContext> variantEncoder =
        new HBaseVariantContextEncoder(sampleNameIndex, vcfHeader);
    GVCFHBase.store(rdd1, variantEncoder, tableName, hbaseContext, splitSize);
    GVCFHBase.store(rdd2, variantEncoder, tableName, hbaseContext, splitSize);

    // load from HBase
    List<T> allVariants = GVCFHBase.load(variantEncoder, tableName, hbaseContext, variantCombiner)
        .collect();
    //allVariants.forEach(System.out::println);

    testUtil.deleteTable(tableName);

    jsc.stop();

    return allVariants;
  }

  /**
   * Validates that the given lists have variant
   * context that correspond to the same variants in the same order.
   * Compares VariantContext by comparing toStringDecodeGenotypes
   */
  public static void assertEqualVariants(final List<VariantContext> v1, final List<VariantContext> v2) {

    // debug
//    System.out.println("Expected");
//    for (int i = 0; i < v1.size(); i++) {
//      System.out.println(v1.get(i).toStringDecodeGenotypes().replaceFirst("\\[VC [^ ]+", "[VC "));
//    }
//
//    System.out.println();
//    System.out.println("Actual");
//    for (int i = 0; i < v2.size(); i++) {
//      System.out.println(v2.get(i).toStringDecodeGenotypes().replaceFirst("\\[VC [^ " +
//          "]+", "[VC "));
//    }

    assertEquals(v1.size(), v2.size());
    for (int i = 0; i < v1.size(); i++) {
      assertEquals(str(v1.get(i)), str(v2.get(i)));
    }
  }

  private static String str(VariantContext v) {
    // strip out source (since it can differ) and attr (since number formatting may differ)
    return v.toStringDecodeGenotypes().replaceFirst("\\[VC [^ ]+", "[VC ").replaceFirst("attr=\\{[^\\}]*\\}", "");
  }

}
