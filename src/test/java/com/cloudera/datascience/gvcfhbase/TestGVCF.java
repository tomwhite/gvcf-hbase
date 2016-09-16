package com.cloudera.datascience.gvcfhbase;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import htsjdk.samtools.util.Locatable;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

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

  private static Iterable<String> print(Tuple2<Locatable, Iterable<VariantContext>> variantsLoc) {
    Locatable loc = variantsLoc._1();
    Iterable<VariantContext> variants = variantsLoc._2();
    StringBuilder sb = new StringBuilder();
    sb.append(loc.getContig()).append(":").append(loc.getStart()).append("-")
        .append(loc.getEnd()).append(",");
    for (VariantContext variant : variants) {
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

  private static Iterable<VariantContext> simpleMindedCombine(Tuple2<Locatable,
      Iterable<VariantContext>> variantsLoc) {
    // assume that variants are the same type, so just combine genotypes
    Locatable loc = variantsLoc._1();
    List<VariantContext> variants = Lists.newArrayList(variantsLoc._2());
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
        "20:1-1,A:G:0/1(1-1),A:G:1/1(1-1)",
        "20:2-4,G:<NON_REF>:0/0(2-7),G:<NON_REF>:0/0(2-7)",
        "20:5-7,G:<NON_REF>:0/0(2-7),G:<NON_REF>:0/0(2-7)", // due to split
        "20:8-8,G:C:1/1(8-8),G:C:0/1(8-8)");

    List<String> allVariants = scan(gvcf1, gvcf2, TestGVCF::print);
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

    List<VariantContext> allVariants = scan(gvcf1, gvcf2, TestGVCF::simpleMindedCombine);
    // compare string representation
    assertEquals(combinedGvcf.stream().map(Object::toString).collect(Collectors.toList()),
        allVariants.stream().map(Object::toString).collect(Collectors.toList()));
  }

  @Test
  public void testSNPAndNonRef() throws Exception {
    ImmutableList<VariantContext> gvcf1 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "G", "a", "0/1"));

    ImmutableList<VariantContext> gvcf2 = ImmutableList.of(
        newVariantContext("20", 1, 1, "A", "<NON_REF>", "b", "0/0"));

    List<String> expectedAllVariants = ImmutableList.of(
        "20:1-1,A:G:0/1(1-1),A:<NON_REF>:0/0(1-1)");

    List<String> allVariants = scan(gvcf1, gvcf2, TestGVCF::print);
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

    List<String> allVariants = scan(gvcf1, gvcf2, TestGVCF::print);
    assertEquals(expectedAllVariants, allVariants);

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

  private <T> List<T> scan(List<VariantContext> gvcf1, List<VariantContext> gvcf2,
      FlatMapFunction<Tuple2<Locatable, Iterable<VariantContext>>, T> f) throws Exception {

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
    List<T> allVariants = GVCFHBase.scan(variantEncoder, tableName, hbaseContext, f)
        .collect();
    //allVariants.forEach(System.out::println);

    testUtil.deleteTable(tableName);

    return allVariants;
  }

}
