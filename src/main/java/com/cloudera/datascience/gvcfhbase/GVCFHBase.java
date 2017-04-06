package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Provides methods to store RDDs of variants in HBase, and load from HBase back into
 * RDDs of variants.
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
      TableName tableName, JavaHBaseContext hbaseContext, int splitSize, JavaSparkContext ctx) {

    // Add the first element from the next partition. Note that this duplicates elements,
    // but that doeesn't matter since HBase will just have duplicate cells with different
    // timestamps - there will not be extra rows. The only case we need to handle is the
    // last element in the last partition.
    JavaRDD<V> rddWithNextElt = withFirstElementFromNextPartition(ctx, rdd);

    bulkPut(hbaseContext, rddWithNextElt, tableName, (FlatMapFunction<V, Put>) v -> {
      if (v == null) {
        System.out.println("woot!");
        return ImmutableList.of();
      }
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
   * Load variants in parallel from HBase and return an RDD of combined variants.
   * @param variantEncoder the encoder to use to convert variants from bytes
   * @param tableName the HBase table name
   * @param hbaseContext the HBase context
   * @param variantCombiner the variant combiner to merge variants together
   * @param <T> the return type; often
   * {@link htsjdk.variant.variantcontext.VariantContext}, when merging variant calls
   * @param <V> the variant type, typically {@link htsjdk.variant.variantcontext.VariantContext}
   * @return an RDD of combined variants
   */
  @SuppressWarnings("unchecked")
  public static <T, V> JavaRDD<T> load(HBaseVariantEncoder<V> variantEncoder,
      TableName tableName, JavaHBaseContext
      hbaseContext, VariantCombiner<V, T> variantCombiner) {
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
                    Iterable<T> values = variantCombiner.finish();
                    Iterables.addAll(buffer, values);
                    if (!buffer.isEmpty()) {
                      continue;
                    }
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
                  for (int i = 0; i < variantsBySampleIndex.size(); i++) {
                    V variant = variantsBySampleIndex.get(i);
                    if (variant != null) { // a variant may be null if it's missing for this position (no call)
                      int keyEnd = variantEncoder.getKeyEnd(variant);
                      if (keyEnd >= rowKey.getStart()) {
                        nextKeyEnd = Math.min(keyEnd, nextKeyEnd);
                      } else {
                        variantsBySampleIndex.set(i, null); // remove any variants or blocks that end before current start (no call)
                      }
                    }
                  }
                  Locatable loc = new Interval(rowKey.getContig(), rowKey.getStart(), nextKeyEnd);
                  System.out.println("combine at " + loc);
                  Iterable<T> values = variantCombiner.combine(loc, variantsBySampleIndex);
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
   * @return an RDD of all the variants for a sample
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
                  V variant = variantEncoder.decodeVariant(rowKey,
                      Iterables.getOnlyElement(result.listCells()), false);
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

  /**
   * For each partition, add the element from the next partition to the end. For the
   * last partition add a null.
   */
  private static <T> JavaRDD<T> withFirstElementFromNextPartition(JavaSparkContext ctx, JavaRDD<T> rdd) {
    int numPartitions = rdd.getNumPartitions();
    // Find the first element in each partition
    List<T> firstEltInEachPartition = rdd
        .mapPartitions((FlatMapFunction<Iterator<T>, T>) it -> toIterable(Iterators.singletonIterator(it.next())))
        .collect();
    // Shift left, so that each partition will be joined with the first element from the
    // _next_ partition
    List<T> firstEltInNextPartition = new ArrayList<T>(firstEltInEachPartition.subList(1,
        numPartitions));
    firstEltInNextPartition.add(null); // the last partition does not have any elements to add to it

    return rdd.zipPartitions(ctx.parallelize(firstEltInNextPartition, numPartitions),
        (FlatMapFunction2<Iterator<T>, Iterator<T>, T>) (it1, it2) -> toIterable(Iterators.concat(it1, it2)));
  }

  // This is not needed with Spark 2
  private static <T> Iterable<T> toIterable(Iterator<T> iterator) {
    return () -> iterator;
  }
}
