package com.cloudera.datascience.gvcfhbase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHBaseVariantEncoder {

  @Test
  public void testRowKey() {
    byte[] key = HBaseVariantEncoder.toRowKeyBytes("20", 400);
    RowKey rowKey = HBaseVariantEncoder.fromRowKeyBytes(key);
    assertEquals("20", rowKey.contig);
    assertEquals(400, rowKey.pos);
  }
}
