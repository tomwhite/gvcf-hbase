package com.cloudera.datascience.gvcfhbase;

public class RowKey {
  String contig;
  int pos;
  public RowKey(String contig, int keyStart) {
    this.contig = contig;
    this.pos = keyStart;
  }
}
