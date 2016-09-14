package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class GVCFHBase {

  public static final byte[] SAMPLE_COLUMN_FAMILY = Bytes.toBytes("s");

  public static <V> void put(JavaRDD<V> rdd, HBaseVariantEncoder<V> variantEncoder,
      TableName tableName, JavaHBaseContext hbaseContext, int splitSize) {
    bulkPut(hbaseContext, rdd, tableName, (FlatMapFunction<V, Put>) v -> {
      List<Put> puts = null;
      int start = variantEncoder.getStart(v);
      int end = variantEncoder.getEnd(v);
      int startSplitIndex = (start - 1) / splitSize; // start and end are 1-based like VCF
      int endSplitIndex = (end - 1) / splitSize;
      if (startSplitIndex == endSplitIndex) {
        puts = ImmutableList.of(variantEncoder.encodeVariant(v));
      } else {
        // break into two variants
        int key2Start = (startSplitIndex + 1) * splitSize + 1;
        int key1End = key2Start - 1;
        V[] vs = variantEncoder.split(v, key1End, key2Start);
        puts = ImmutableList.of(variantEncoder.encodeVariant(vs[0]),
            variantEncoder.encodeVariant(vs[1]));
      }
      return puts;
    });
  }

  @SuppressWarnings("unchecked")
  public static <T, V> JavaRDD<T> scan(HBaseVariantEncoder<V> variantEncoder,
      TableName tableName, JavaHBaseContext
      hbaseContext, Function2<RowKey, Iterable<V>, T> f) {
    Scan scan = new Scan();
    scan.setCaching(100);
    return hbaseContext.hbaseRDD(tableName, scan)
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,
            Result>>, T>) rows -> {
          List<T> output = new ArrayList<>();
          List<V> variantsBySampleIndex = new ArrayList<>();
          int numSamples = -1;
          while (rows.hasNext()) {
            Tuple2<ImmutableBytesWritable, Result> row = rows.next();
            Result result = row._2();
            // determine number of samples from first row in split,
            // since they all have an entry there
            if (numSamples == -1) {
              numSamples = result.listCells().size();
              variantsBySampleIndex = Arrays.asList((V[]) new Object[numSamples]);
            }
            RowKey rowKey = variantEncoder.fromRowKeyBytes(result.getRow());
            for (Cell cell : result.listCells()) {
              V variant = variantEncoder.decodeVariant(rowKey, cell);
              variantsBySampleIndex.set(variantEncoder.getSampleIndex(variant), variant);
            }
            int nextKeyEnd = Integer.MAX_VALUE; // how many positions we can
            // iterate over before the next row
            for (V variant : variantsBySampleIndex) {
              nextKeyEnd = Math.min(variantEncoder.getKeyEnd(variant), nextKeyEnd);
            }

            List<V> variants = variantEncoder.adjustEnds(variantsBySampleIndex,
                rowKey.pos, nextKeyEnd);

            output.add(f.call(rowKey, variants));
          }
          return output;
        });
  }

  /**
   * Like {@link JavaHBaseContext#bulkPut(JavaRDD, TableName, Function)}, but allows
   * multiple puts per entry in the RDD.
   *
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
}
