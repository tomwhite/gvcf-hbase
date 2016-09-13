package com.cloudera.datascience.gvcfhbase;

import java.io.Serializable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseVariantLiteEncoder extends HBaseVariantEncoder<VariantLite>
    implements Serializable {
  @Override
  public Put encodeVariant(VariantLite variant) {
    // note that we only store one genotype here as we expect to load single sample
    // gvcf files
    GenotypeLite genotype = variant.getGenotype();
    int start = variant.getStart();
    int end = variant.getEnd();
    int logicalStart = variant.getLogicalStart();
    int logicalEnd = variant.getLogicalEnd();
    byte[] rowKey = toRowKeyBytes(variant.getContig(), logicalStart);
    Put put = new Put(rowKey);
    byte[] qualifier = Bytes.toBytes(genotype.getSampleIndex());
    String val = logicalEnd + "," + start + "," + end + "," + genotype.getValue();
    byte[] value = Bytes.toBytes(val);
    put.addColumn(GVCFHBase.SAMPLE_COLUMN_FAMILY, qualifier, value);
    return put;
  }

  @Override
  public VariantLite decodeVariant(RowKey rowKey, Cell cell) {
    int sampleIndex = Bytes.toInt(cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
    String[] splits = val.split(",");
    int logicalEnd = Integer.parseInt(splits[0]);
    int start = Integer.parseInt(splits[1]);
    int end = Integer.parseInt(splits[2]);
    GenotypeLite genotype = new GenotypeLite(sampleIndex, splits[3]);
    return new VariantLite(rowKey.contig, start, end, rowKey.pos, logicalEnd,
        genotype);
  }

  @Override
  public boolean isRefPosition(RowKey rowKey, VariantLite variant) {
    return !variant.getGenotype().getValue().equals("N/A") &&
        rowKey.pos == variant.getStart();
  }

  @Override
  public int getSampleIndex(VariantLite variant) {
    return variant.getGenotype().getSampleIndex();
  }

  @Override
  public int getStart(VariantLite variant) {
    return variant.getStart();
  }

  @Override
  public int getEnd(VariantLite variant) {
    return variant.getEnd();
  }

  @Override
  public int getLogicalEnd(VariantLite variant) {
    return variant.getLogicalEnd();
  }

  @Override
  public VariantLite[] split(VariantLite variant, int midStart, int midEnd) {
    String contig = variant.getContig();
    int start = variant.getStart();
    int end = variant.getEnd();
    return new VariantLite[] {
        new VariantLite(contig, start, end, start, midEnd, variant.getGenotype()),
        new VariantLite(contig, start, end, midStart, end, variant.getGenotype())
    };
  }
}
