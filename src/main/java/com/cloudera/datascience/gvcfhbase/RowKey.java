package com.cloudera.datascience.gvcfhbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The HBase row key, made up of the contig and the start position.
 */
class RowKey {
  private static final int CONTIG_LENGTH = 2;

  private final String contig;
  private final int start;

  public RowKey(String contig, int keyStart) {
    this.contig = contig;
    this.start = keyStart;
  }

  public String getContig() {
    return contig;
  }

  public int getStart() {
    return start;
  }

  public static byte[] toRowKeyBytes(String contig, int keyStart) {
    byte[] row = new byte[CONTIG_LENGTH + Bytes.SIZEOF_INT];
    Bytes.putBytes(row, 0, Bytes.toBytes(StringUtils.leftPad(contig, 2)), 0, CONTIG_LENGTH);
    Bytes.putInt(row, CONTIG_LENGTH, keyStart);
    return row;
  }

  public static RowKey fromRowKeyBytes(byte[] row) {
    return new RowKey(
        Bytes.toString(row, 0, CONTIG_LENGTH).trim(),
        Bytes.toInt(row, CONTIG_LENGTH)
    );
  }

  @Override
  public String toString() {
    return "RowKey{" +
        "contig='" + contig + '\'' +
        ", start=" + start +
        '}';
  }
}
