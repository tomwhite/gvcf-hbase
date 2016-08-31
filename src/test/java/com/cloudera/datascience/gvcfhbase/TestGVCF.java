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
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
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
    byte[][] columnFamilies = new byte[][] { SAMPLE_COLUMN_FAMILY };
    byte[][] splitKeys = new byte[][] { Bytes.toBytes(5) };
    HTable table = testUtil.createTable(tableName, columnFamilies, splitKeys);

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
        new VariantLite(4, 6, new GenotypeLite(1, "0|0")),
        new VariantLite(7, 8, new GenotypeLite(1, "N/A")));

    JavaRDD<VariantLite> rdd1 = jsc.parallelize(gvcf1);
    JavaRDD<VariantLite> rdd2 = jsc.parallelize(gvcf2);

    // insert into HBase

    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    int splitSize = 4;
    put(rdd1, tableName, hbaseContext, splitSize);
    put(rdd2, tableName, hbaseContext, splitSize);

    // Scan over all positions
    List<String> allPositions = scan(tableName, hbaseContext, true, TestGVCF::process).collect();
    //allPositions.forEach(System.out::println);
    List<String> expectedAllPositions = ImmutableList.of(
        "1,0|0(end=1),0|1(end=3)",
        "2,N/A(end=7),0|1(end=3)",
        "3,N/A(end=7),0|1(end=3)",
        "4,N/A(end=7),0|0(end=6)",
        "5,N/A(end=7),0|0(end=6)",
        "6,N/A(end=7),0|0(end=6)",
        "7,N/A(end=7),N/A(end=8)",
        "8,1|1(end=8),N/A(end=8)");
    assertEquals(expectedAllPositions, allPositions);

//    // Scan over variants only
//    List<String> allVariants = scan(tableName, hbaseContext, false, TestGVCF::process).collect();
//    //allVariants.forEach(System.out::println);
//    List<String> expectedAllVariants = ImmutableList.of(
//        "1,0|0(end=1),0|1(end=3)",
//        "4,N/A(end=7),0|0(end=6)",
//        "8,1|1(end=8),N/A(end=8)");
//    assertEquals(expectedAllVariants, allVariants);

    testUtil.deleteTable(tableName);
  }

  /**
   * Like {@link JavaHBaseContext#bulkPut(JavaRDD, TableName, Function)}, but allows
   * multiple puts per entry in the RDD.
   * @param <T>
   * @param hbaseContext
   * @param rdd
   * @param tableName
   * @param f
   */
  private static <T> void bulkPut(JavaHBaseContext hbaseContext, JavaRDD<T> rdd,
      TableName tableName, FlatMapFunction<T, Put> f) {
    final byte[] tName = tableName.getName();
    hbaseContext.foreachPartition(rdd, (VoidFunction<Tuple2<Iterator<T>, Connection>>)
        tuple -> {
          Connection connection = tuple._2();
          BufferedMutator m = connection.getBufferedMutator(TableName.valueOf(tName));
          try {
            for (Iterator<T> i = tuple._1(); i.hasNext(); ) {
              for (Put put : f.call(i.next())) {
                m.mutate(put);
              }
            }
          } finally {
            m.flush();
            m.close();
          }
        });
  }

  public void put(JavaRDD<VariantLite> rdd, TableName tableName, JavaHBaseContext
      hbaseContext, int splitSize) {
    // TODO: can we use bulkLoad for efficiency (need to port interface to Java)
    bulkPut(hbaseContext, rdd, tableName, (FlatMapFunction<VariantLite, Put>) v -> {
      List<Put> puts = null;
      int start = v.getStart();
      int end = v.getEnd();
      int startSplitIndex = (start - 1) / splitSize; // start and end are 1-based like VCF
      int endSplitIndex = (end - 1) / splitSize;
      if (startSplitIndex == endSplitIndex) {
        puts = ImmutableList.of(encodeVariant(v));
      } else {
        // break into two logical variants
        int midStart = (startSplitIndex + 1) * splitSize + 1;
        int midEnd = midStart - 1;
        VariantLite v1 = new VariantLite(start, end, start, midEnd, v.getGenotype());
        VariantLite v2 = new VariantLite(start, end, midStart, end, v.getGenotype());
        puts = ImmutableList.of(encodeVariant(v1), encodeVariant(v2));
      }
      return puts;
    });
  }

  private Put encodeVariant(VariantLite variant) {
    // note that we only store one genotype here as we expect to load single sample
    // gvcf files
    GenotypeLite genotype = variant.getGenotype();
    int start = variant.getStart();
    int end = variant.getEnd();
    int logicalStart = variant.getLogicalStart();
    int logicalEnd = variant.getLogicalEnd();
    Put put = new Put(Bytes.toBytes(logicalStart));
    byte[] qualifier = Bytes.toBytes(genotype.getSampleIndex());
    String val = logicalEnd + "," + start + "," + end + "," + genotype.getValue();// poor encoding!
    byte[] value = Bytes.toBytes(val);
    put.addColumn(SAMPLE_COLUMN_FAMILY, qualifier, value);
    System.out.println("put: " + genotype.getSampleIndex() + ":" + logicalStart + "(" +
        logicalEnd +
        ") "
        + genotype
        .getValue());
    return put;
  }

  private VariantLite decodeVariant(int logicalStart, Cell cell) {
    int sampleIndex = Bytes.toInt(cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
    String[] splits = val.split(",");
    int logicalEnd = Integer.parseInt(splits[0]);
    int start = Integer.parseInt(splits[1]);
    int end = Integer.parseInt(splits[2]);
    GenotypeLite genotype = new GenotypeLite(sampleIndex, splits[3]);
    return new VariantLite(start, end, logicalStart, logicalEnd, genotype);
  }

  public <T> JavaRDD<T> scan(TableName tableName, JavaHBaseContext
      hbaseContext, boolean allPositions, Function2<Integer, Iterable<VariantLite>, T> f) {
    Scan scan = new Scan();
    scan.setCaching(100);
    return hbaseContext.hbaseRDD(tableName, scan)
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,
            Result>>, T>) rows -> {
          List<T> output = new ArrayList<>();
          List<VariantLite> variantsBySampleIndex = new ArrayList<>();
          int numSamples = -1;
          while (rows.hasNext()) {
            Tuple2<ImmutableBytesWritable, Result> row = rows.next();
            Result result = row._2();
            if (numSamples == -1) { // determine number of samples from first row,
              // since they all have an entry there TODO: use column family metadata here
              numSamples = result.listCells().size();
              System.out.println("tw: num samples : " + numSamples);
              System.out.println("tw: row key: " + Bytes.toInt(result.getRow()));
              variantsBySampleIndex = Arrays.asList(new VariantLite[numSamples]);
            }
            int logicalStart = Bytes.toInt(result.getRow());
            boolean isVariantPos = false;
            for (Cell cell : result.listCells()) {
              VariantLite variant = decodeVariant(logicalStart, cell);
              variantsBySampleIndex.set(variant.getGenotype().getSampleIndex(), variant);
              if (!variant.getGenotype().getValue().equals("N/A")) {
                isVariantPos = true;
              }
            }
            int nextLogicalEnd = Integer.MAX_VALUE; // how many positions we can iterate over before the next row
            for (VariantLite variant : variantsBySampleIndex) {
              nextLogicalEnd = Math.min(variant.getLogicalEnd(), nextLogicalEnd);
            }

            if (allPositions) {
              for (int pos = logicalStart; pos <= nextLogicalEnd; pos++) {
                output.add(f.call(pos, variantsBySampleIndex));
              }
            } else if (isVariantPos) {
              output.add(f.call(logicalStart, variantsBySampleIndex));
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
