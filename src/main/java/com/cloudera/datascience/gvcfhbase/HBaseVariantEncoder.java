package com.cloudera.datascience.gvcfhbase;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

/**
 * An interface for encapsulating the way that a variant object (of type <code>V</code>)
 * is converted to and from the HBase bytes representation.
 * @param <V>
 */
public abstract class HBaseVariantEncoder<V> {
  public abstract int getNumSamples();
  public abstract Put encodeVariant(V variant) throws IOException;
  public abstract Put encodeNoCallFollowing(V prevVariant) throws IOException;
  public abstract V decodeVariant(RowKey rowKey, Cell cell, boolean includeKeyAttributes) throws IOException;
  public abstract int getSampleIndex(Cell cell);
  public abstract int getStart(V variant);
  public abstract int getEnd(V variant);
  public abstract int getKeyEnd(V variant);
  public abstract V[] split(V v, int key1End, int key2Start);
}
