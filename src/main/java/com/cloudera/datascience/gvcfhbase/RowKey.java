package com.cloudera.datascience.gvcfhbase;

public class RowKey {
  String contig;
  int pos;
  public RowKey(String contig, int logicalStart) {
    this.contig = contig;
    this.pos = logicalStart;
  }
}
