package com.cloudera.datascience.gvcfhbase;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An interface for encapsulating the way that a variant object (of type <code>V</code>)
 * is converted to and from the HBase bytes representation.
 * @param <V>
 */
public abstract class HBaseVariantEncoder<V> {
  public abstract int getNumSamples();
  public abstract Put encodeVariant(V variant);
  public abstract V decodeVariant(RowKey rowKey, Cell cell);
  public abstract int getSampleIndex(V variant);
  public abstract int getStart(V variant);
  public abstract int getEnd(V variant);
  public abstract int getKeyEnd(V variant);
  public abstract V[] split(V v, int key1End, int key2Start);
  public abstract List<V> adjustEnds(List<V> variantsBySampleIndex, int start, int nextKeyEnd);
}
