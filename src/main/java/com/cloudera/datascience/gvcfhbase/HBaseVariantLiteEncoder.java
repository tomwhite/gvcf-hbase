package com.cloudera.datascience.gvcfhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseVariantLiteEncoder implements HBaseVariantEncoder<VariantLite> {
  @Override
  public Put encodeVariant(VariantLite variant) {
    // note that we only store one genotype here as we expect to load single sample
    // gvcf files
    GenotypeLite genotype = variant.getGenotype();
    int start = variant.getStart();
    int end = variant.getEnd();
    int logicalStart = variant.getLogicalStart();
    int logicalEnd = variant.getLogicalEnd();
    Put put = new Put(Bytes.toBytes(logicalStart));
    byte[] qualifier = Bytes.toBytes(genotype.getSampleIndex());
    // TODO: improve encoding
    String val = logicalEnd + "," + start + "," + end + "," + genotype.getValue();
    byte[] value = Bytes.toBytes(val);
    put.addColumn(GVCFHBase.SAMPLE_COLUMN_FAMILY, qualifier, value);
    return put;
  }

  @Override
  public VariantLite decodeVariant(int logicalStart, Cell cell) {
    int sampleIndex = Bytes.toInt(cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
    String[] splits = val.split(",");
    int logicalEnd = Integer.parseInt(splits[0]);
    int start = Integer.parseInt(splits[1]);
    int end = Integer.parseInt(splits[2]);
    GenotypeLite genotype = new GenotypeLite(sampleIndex, splits[3]);
    return new VariantLite(start, end, logicalStart, logicalEnd, genotype);
  }

  @Override
  public boolean isRefPosition(int logicalStart, VariantLite variant) {
    return !variant.getGenotype().getValue().equals("N/A") &&
        logicalStart == variant.getStart();
  }
}
