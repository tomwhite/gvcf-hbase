package com.cloudera.datascience.gvcfhbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKey {
  private static final int CONTIG_LENGTH = 2;
  public String contig;
  public int pos;
  public RowKey(String contig, int keyStart) {
    this.contig = contig;
    this.pos = keyStart;
  }

  public static byte[] getSplitKeyBytes(String contig, int keyStart) {
    return toRowKeyBytes(contig, keyStart);
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
}
