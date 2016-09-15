package com.cloudera.datascience.gvcfhbase;

public class RowKey {
  public String contig;
  public int pos;
  public RowKey(String contig, int keyStart) {
    this.contig = contig;
    this.pos = keyStart;
  }
}
