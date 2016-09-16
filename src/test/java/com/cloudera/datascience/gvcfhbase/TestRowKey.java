package com.cloudera.datascience.gvcfhbase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestRowKey {

  @Test
  public void testRowKey() {
    byte[] key = RowKey.toRowKeyBytes("20", 400);
    RowKey rowKey = RowKey.fromRowKeyBytes(key);
    assertEquals("20", rowKey.getContig());
    assertEquals(400, rowKey.getStart());
  }
}
