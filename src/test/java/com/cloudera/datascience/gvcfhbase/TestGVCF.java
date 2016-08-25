package com.cloudera.datascience.gvcfhbase;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
        new VariantLite(2, 7, new GenotypeLite("a", "N/A")),
        new VariantLite(8, 8, new GenotypeLite("a", "1|1")),
        new VariantLite(9, 12, new GenotypeLite("a", "N/A")),
        new VariantLite(13, 15, new GenotypeLite("a", "0|1")));

    ImmutableList<VariantLite> gvcf2 = ImmutableList.of(
        new VariantLite(1, 3, new GenotypeLite("b", "0|1")),
        new VariantLite(4, 6, new GenotypeLite("b", "N/A")),
        new VariantLite(7, 9, new GenotypeLite("b", "0|0")),
        new VariantLite(10, 15, new GenotypeLite("b", "N/A")));

    JavaRDD<VariantLite> rdd = jsc.parallelize(gvcf1);

    // insert into HBase
    // TODO: can we use bulkLoad for efficiency (need to port interface to Java)
    Configuration conf = testUtil.getConfiguration();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    hbaseContext.bulkPut(rdd, tableName, (Function<VariantLite, Put>) v -> {
      Put put = new Put(Bytes.toBytes(v.getStart()));
      byte[] qualifier = Bytes.toBytes(v.getGenotype().getSampleName());
      String val = v.getEnd() + "," + v.getGenotype().getValue(); // poor encoding!
      byte[] value = Bytes.toBytes(val);
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
    JavaRDD<String> r = hbaseContext.hbaseRDD(tableName, scan)
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,
            Result>>, String>) rows -> {
          List<String> output = Lists.newArrayList();
          Map<String, VariantLite> sampleToVariants = Maps.newLinkedHashMap(); // keep
          // track of current variants overlapping pos (TODO: think about how samples are
          // ordered)
          while (rows.hasNext()) {
            Tuple2<ImmutableBytesWritable, Result> row = rows.next();
            Result result = row._2();
            int start = Bytes.toInt(result.getRow());
            int nextEnd = Integer.MAX_VALUE; // position just before we need to read
            // next row
            for (Cell cell : result.listCells()) {
              String sampleName = Bytes.toString(cell.getQualifierArray(),
                  cell.getQualifierOffset(), cell.getQualifierLength());
              String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                  cell.getValueLength());
              String[] splits = val.split(",");
              int end = Integer.parseInt(splits[0]);
              nextEnd = Math.min(end, nextEnd);
              GenotypeLite genotype = new GenotypeLite(sampleName, splits[1]);
              VariantLite variant = new VariantLite(start, end, genotype);
              sampleToVariants.put(sampleName, variant);
            }

            for (int pos = start; pos <= nextEnd; pos++) {
              StringBuilder sb = new StringBuilder();
              sb.append(pos).append(",");
              for (Map.Entry<String, VariantLite> entry : sampleToVariants.entrySet()) {
                VariantLite variant = entry.getValue();
                GenotypeLite genotype = variant.getGenotype();
                sb.append(genotype.getValue());
                sb.append("(end=").append(variant.getEnd()).append(")");
                output.add(sb.toString());
              }
            }
          }
          return output;
        });
    List<String> collect = r.collect();
    System.out.println(collect);
    assertEquals("1,0|0(end=1)", collect.get(0));

    testUtil.deleteTable(tableName);
  }
}
