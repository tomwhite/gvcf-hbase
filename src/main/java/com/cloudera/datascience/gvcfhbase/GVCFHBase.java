package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Provides methods to store gVCF data in HBase, and scan over it.
 */
public class GVCFHBase {

  public static final byte[] SAMPLE_COLUMN_FAMILY = Bytes.toBytes("s");

  /**
   * Store variants in an HBase table.
   * @param rdd the RDD of variants, typically loaded from a gVCF file.
   * @param variantEncoder the encoder to use to convert variants to bytes
   * @param tableName the HBase table name, must already exist
   * @param hbaseContext the HBase context
   * @param splitSize the size (in genomic locus positions) of HBase table regions;
   *                  tables must be pre-split
   * @param <V> the variant type, typically {@link htsjdk.variant.variantcontext.VariantContext}
   */
  public static <V> void store(JavaRDD<V> rdd, HBaseVariantEncoder<V> variantEncoder,
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

  /**
   * Load variants in parallel from HBase and return an RDD of consolidated variants.
   * @param variantEncoder the encoder to use to convert variants from bytes
   * @param tableName the HBase table name
   * @param hbaseContext the HBase context
   * @param f the consolidation function. Takes a genomic range (where the start is the
   *          start position for all the variants returned, and the end is the position
   *          before the start position of the next row) and a collection of variants;
   *          the return value is zero or more consolidated values (e.g.
   *          {@link htsjdk.variant.variantcontext.VariantContext}).
   *          May be stateful, in order to perform sophisticated merging.
   * @param <T> the return type; often
   * {@link htsjdk.variant.variantcontext.VariantContext}, when merging variant calls
   * @param <V> the variant type, typically {@link htsjdk.variant.variantcontext.VariantContext}
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <T, V> JavaRDD<T> load(HBaseVariantEncoder<V> variantEncoder,
      TableName tableName, JavaHBaseContext
      hbaseContext, FlatMapFunction<Tuple2<Locatable, Iterable<V>>, T> f) {
    Scan scan = new Scan();
    scan.setCaching(100);
    return hbaseContext.hbaseRDD(tableName, scan)
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,
            Result>>, T>) rows -> {
          int numSamples = variantEncoder.getNumSamples();
          final List<V> variantsBySampleIndex = Arrays.asList((V[]) new Object[numSamples]);
          Iterator<T> it = new AbstractIterator<T>() {
            LinkedList<T> buffer = new LinkedList<T>();
            @Override
            protected T computeNext() {
              while (true) {
                try {
                  if (!buffer.isEmpty()) {
                    return buffer.removeFirst();
                  }
                  if (!rows.hasNext()) {
                    return endOfData();
                  }
                  Tuple2<ImmutableBytesWritable, Result> row = rows.next();
                  Result result = row._2();
                  RowKey rowKey = RowKey.fromRowKeyBytes(result.getRow());
                  for (Cell cell : result.listCells()) {
                    V variant = variantEncoder.decodeVariant(rowKey, cell, true);
                    variantsBySampleIndex.set(variantEncoder.getSampleIndex(variant), variant);
                  }
                  // how many positions we can iterate over before the next row
                  int nextKeyEnd = Integer.MAX_VALUE;
                  for (V variant : variantsBySampleIndex) {
                    nextKeyEnd = Math.min(variantEncoder.getKeyEnd(variant), nextKeyEnd);
                  }

                  Iterable<T> values = f.call(
                      new Tuple2<>(
                          new Interval(rowKey.getContig(), rowKey.getStart(), nextKeyEnd),
                          variantsBySampleIndex));
                  Iterables.addAll(buffer, values);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }
          };
          return (Iterable<T>) () -> it;
        });
  }

  /**
   * Load variants in parallel from HBase for a single sample.
   * @param variantEncoder the encoder to use to convert variants from bytes
   * @param tableName the HBase table name
   * @param hbaseContext the HBase context
   * @param sampleName the sample name to return all variants for
   * @param sampleNameIndex the global sample name index
   * {@link htsjdk.variant.variantcontext.VariantContext}, when merging variant calls
   * @param <V> the variant type, typically {@link htsjdk.variant.variantcontext.VariantContext}
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <V> JavaRDD<V> loadSingleSample(HBaseVariantEncoder<V> variantEncoder,
      TableName tableName, JavaHBaseContext hbaseContext, String sampleName,
      SampleNameIndex sampleNameIndex) {
    Scan scan = new Scan();
    byte[] qualifier = Bytes.toBytes(sampleNameIndex.getSampleIndex(sampleName));
    scan.addColumn(SAMPLE_COLUMN_FAMILY, qualifier);
    scan.setCaching(100);
    return hbaseContext.hbaseRDD(tableName, scan)
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,
            Result>>, V>) rows -> {
          Iterator<V> it = new AbstractIterator<V>() {
            @Override
            protected V computeNext() {
              while (true) {
                if (!rows.hasNext()) {
                  return endOfData();
                }
                Tuple2<ImmutableBytesWritable, Result> row = rows.next();
                Result result = row._2();
                RowKey rowKey = RowKey.fromRowKeyBytes(result.getRow());
                try {
                  V variant = variantEncoder.decodeVariant(rowKey, Iterables
                      .getOnlyElement(result.listCells()), false);
                  if (variantEncoder.getStart(variant) != rowKey.getStart()) { // ignore
                    // fake variant from split
                    continue;
                  }
                  return variant;
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            }
          };
          return (Iterable<V>) () -> it;
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
