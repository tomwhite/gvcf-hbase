package com.cloudera.datascience.gvcfhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

/**
 * An interface for encapsulating the way that a variant object (of type <code>V</code>)
 * is converted to and from the HBase bytes representation.
 * @param <V>
 */
public interface HBaseVariantEncoder<V> {
  Put encodeVariant(V variant);
  V decodeVariant(int logicalStart, Cell cell);
  boolean isRefPosition(int logicalStart, V variant);
}
