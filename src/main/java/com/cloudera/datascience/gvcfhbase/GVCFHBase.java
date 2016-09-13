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

  private static HBaseVariantEncoder<VariantLite> variantEncoder =
      new HBaseVariantLiteEncoder();

  public static void put(JavaRDD<VariantLite> rdd, TableName tableName, JavaHBaseContext
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

  public static <T> JavaRDD<T> scan(TableName tableName, JavaHBaseContext
      hbaseContext, boolean allPositions, Function2<Integer, Iterable<VariantLite>, T>
      f) {
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
            // determine number of samples from first row in split,
            // since they all have an entry there
            if (numSamples == -1) {
              numSamples = result.listCells().size();
              variantsBySampleIndex = Arrays.asList(new VariantLite[numSamples]);
            }
            int logicalStart = Bytes.toInt(result.getRow());
            boolean isVariantPos = false;
            for (Cell cell : result.listCells()) {
              VariantLite variant = decodeVariant(logicalStart, cell);
              variantsBySampleIndex.set(variant.getGenotype().getSampleIndex(), variant);
              if (isRefPosition(logicalStart, variant)) {
                isVariantPos = true;
              }
            }
            int nextLogicalEnd = Integer.MAX_VALUE; // how many positions we can
            // iterate over before the next row
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

  // TODO: move following into an interface

  private static Put encodeVariant(VariantLite variant) {
    return variantEncoder.encodeVariant(variant);
  }

  private static VariantLite decodeVariant(int logicalStart, Cell cell) {
    return variantEncoder.decodeVariant(logicalStart, cell);
  }

  private static boolean isRefPosition(int logicalStart, VariantLite variant) {
    return variantEncoder.isRefPosition(logicalStart, variant);
  }
}
