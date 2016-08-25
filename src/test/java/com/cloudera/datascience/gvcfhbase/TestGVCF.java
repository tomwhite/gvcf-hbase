package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import static org.junit.Assert.assertEquals;

public class TestGVCF implements Serializable {

  private static final byte[] SAMPLE_COLUMN_FAMILY = Bytes.toBytes("s");
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
    HTable table = testUtil.createTable(tableName, SAMPLE_COLUMN_FAMILY);

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
        new VariantLite(1, 1, new GenotypeLite(0, "0|0")),
        new VariantLite(2, 7, new GenotypeLite(0, "N/A")),
        new VariantLite(8, 8, new GenotypeLite(0, "1|1")));

    ImmutableList<VariantLite> gvcf2 = ImmutableList.of(
        new VariantLite(1, 3, new GenotypeLite(1, "0|1")),
        new VariantLite(4, 6, new GenotypeLite(1, "N/A")),
        new VariantLite(7, 8, new GenotypeLite(1, "0|0")));

    JavaRDD<VariantLite> rdd1 = jsc.parallelize(gvcf1);
    JavaRDD<VariantLite> rdd2 = jsc.parallelize(gvcf2);

    // insert into HBase

    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    put(rdd1, tableName, hbaseContext);
    put(rdd2, tableName, hbaseContext);

    // Scan over all positions
    List<String> allPositions = scan(tableName, hbaseContext, true).collect();
    //allPositions.forEach(System.out::println);
    List<String> expectedAllPositions = ImmutableList.of(
        "1,0|0(end=1),0|1(end=3)",
        "2,N/A(end=7),0|1(end=3)",
        "3,N/A(end=7),0|1(end=3)",
        "4,N/A(end=7),N/A(end=6)",
        "5,N/A(end=7),N/A(end=6)",
        "6,N/A(end=7),N/A(end=6)",
        "7,N/A(end=7),0|0(end=8)",
        "8,1|1(end=8),0|0(end=8)");
    assertEquals(expectedAllPositions, allPositions);

    // Scan over variants only
    List<String> allVariants = scan(tableName, hbaseContext, false).collect();
    //allVariants.forEach(System.out::println);
    List<String> expectedAllVariants = ImmutableList.of(
        "1,0|0(end=1),0|1(end=3)",
        "7,N/A(end=7),0|0(end=8)",
        "8,1|1(end=8),0|0(end=8)");
    assertEquals(expectedAllVariants, allVariants);

    testUtil.deleteTable(tableName);
  }

  public void put(JavaRDD<VariantLite> rdd, TableName tableName, JavaHBaseContext
      hbaseContext) {
    // TODO: can we use bulkLoad for efficiency (need to port interface to Java)
    hbaseContext.bulkPut(rdd, tableName, (Function<VariantLite, Put>) v -> {
      Put put = new Put(Bytes.toBytes(v.getStart()));
      byte[] qualifier = Bytes.toBytes(v.getGenotype().getSampleIndex());
      String val = v.getEnd() + "," + v.getGenotype().getValue(); // poor encoding!
      byte[] value = Bytes.toBytes(val);
      put.addColumn(SAMPLE_COLUMN_FAMILY, qualifier, value);
      return put;
    });
  }

  public JavaRDD<String> scan(TableName tableName, JavaHBaseContext
      hbaseContext, boolean allPositions) {
    Scan scan = new Scan();
    scan.setCaching(100);
    return hbaseContext.hbaseRDD(tableName, scan)
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,
            Result>>, String>) rows -> {
          List<String> output = new ArrayList<>();
          List<VariantLite> variantsBySampleIndex = new ArrayList<>();
          int numSamples = -1;
          while (rows.hasNext()) {
            Tuple2<ImmutableBytesWritable, Result> row = rows.next();
            Result result = row._2();
            if (numSamples == -1) { // determine number of samples from first row,
              // since they all have an entry there
              numSamples = result.listCells().size();
              variantsBySampleIndex = Arrays.asList(new VariantLite[numSamples]);
            }
            int start = Bytes.toInt(result.getRow());
            boolean isVariantPos = false;
            for (Cell cell : result.listCells()) {
              int sampleIndex = Bytes.toInt(cell.getQualifierArray(),
                  cell.getQualifierOffset(), cell.getQualifierLength());
              String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                  cell.getValueLength());
              String[] splits = val.split(",");
              int end = Integer.parseInt(splits[0]);
              GenotypeLite genotype = new GenotypeLite(sampleIndex, splits[1]);
              VariantLite variant = new VariantLite(start, end, genotype);
              variantsBySampleIndex.set(sampleIndex, variant);
              if (!variant.getGenotype().getValue().equals("N/A")) {
                isVariantPos = true;
              }
            }
            int nextEnd = Integer.MAX_VALUE; // how many positions we can iterate over
            // before the next row
            for (VariantLite variant : variantsBySampleIndex) {
              nextEnd = Math.min(variant.getEnd(), nextEnd);
            }

            if (allPositions) {
              for (int pos = start; pos <= nextEnd; pos++) {
                output.add(process(pos, variantsBySampleIndex));
              }
            } else if (isVariantPos) {
              output.add(process(start, variantsBySampleIndex));
            }
          }
          return output;
        });
  }

  private static String process(int pos, Iterable<VariantLite> variants) {
    StringBuilder sb = new StringBuilder();
    sb.append(pos).append(",");
    for (VariantLite variant : variants) {
      GenotypeLite genotype = variant.getGenotype();
      sb.append(genotype.getValue());
      sb.append("(end=").append(variant.getEnd()).append(")");
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

}
