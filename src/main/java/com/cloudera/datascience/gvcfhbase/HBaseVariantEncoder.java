package com.cloudera.datascience.gvcfhbase;

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
  public abstract Put encodeVariant(V variant);
  public abstract V decodeVariant(RowKey rowKey, Cell cell);
  public abstract boolean isRefPosition(RowKey rowKey, V variant);
  public abstract int getSampleIndex(V variant);
  public abstract int getStart(V variant);
  public abstract int getEnd(V variant);
  public abstract int getKeyEnd(V variant);
  public abstract V[] split(V v, int key1End, int key2Start);

  private static final int CONTIG_LENGTH = 2;

  public byte[] toRowKeyBytes(String contig, int keyStart) {
    byte[] row = new byte[CONTIG_LENGTH + Bytes.SIZEOF_INT];
    Bytes.putBytes(row, 0, Bytes.toBytes(StringUtils.leftPad(contig, 2)), 0, CONTIG_LENGTH);
    Bytes.putInt(row, CONTIG_LENGTH, keyStart);
    return row;
  }

  public RowKey fromRowKeyBytes(byte[] row) {
    return new RowKey(
        Bytes.toString(row, 0, CONTIG_LENGTH).trim(),
        Bytes.toInt(row, CONTIG_LENGTH)
    );
  }
}
