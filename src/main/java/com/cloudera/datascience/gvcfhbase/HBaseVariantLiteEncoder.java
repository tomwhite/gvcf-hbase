package com.cloudera.datascience.gvcfhbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
    int keyStart = variant.getKeyStart();
    int keyEnd = variant.getKeyEnd();
    String ref = variant.getRef();
    String alt = variant.getAlt();
    byte[] rowKey = toRowKeyBytes(variant.getContig(), keyStart);
    Put put = new Put(rowKey);
    byte[] qualifier = Bytes.toBytes(genotype.getSampleIndex());
    String val = keyEnd + "," + start + "," + end + "," + ref + "," + alt + "," +
        genotype.getValue();
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
    int keyEnd = Integer.parseInt(splits[0]);
    int start = Integer.parseInt(splits[1]);
    int end = Integer.parseInt(splits[2]);
    String ref = splits[3];
    String alt = splits[4];
    GenotypeLite genotype = new GenotypeLite(sampleIndex, splits[5]);
    return new VariantLite(rowKey.contig, start, end, ref, alt,
        rowKey.pos, keyEnd, genotype);
  }

  @Override
  public boolean isRefPosition(RowKey rowKey, VariantLite variant) {
    return !variant.getAlt().equals("<NON_REF>") &&
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
  public int getKeyEnd(VariantLite variant) {
    return variant.getKeyEnd();
  }

  @Override
  public VariantLite[] split(VariantLite variant, int key1End, int key2Start) {
    String contig = variant.getContig();
    int start = variant.getStart();
    int end = variant.getEnd();
    String ref = variant.getRef();
    String alt = variant.getAlt();
    return new VariantLite[] {
        new VariantLite(contig, start, end, ref, alt, start, key1End, variant.getGenotype()),
        new VariantLite(contig, start, end, ref, alt, key2Start, end, variant.getGenotype())
    };
  }

  @Override
  public List<VariantLite> adjustEnds(List<VariantLite> variantsBySampleIndex, int start, int nextKeyEnd) {
    List<VariantLite> variants = new ArrayList<>();
    for (VariantLite v : variantsBySampleIndex) {
      if (v.getAlt().equals("<NON_REF>")) {
        String ref = start == v.getStart() ? v.getRef() : "."; // unknown ref (TODO:use fasta)
        int end = nextKeyEnd < v.getEnd() ? nextKeyEnd : v.getEnd();
        VariantLite variant = new VariantLite(v.getContig(), start, end,
            ref, v.getAlt(), v.getKeyStart(), v.getKeyEnd(), v.getGenotype());
        variants.add(variant);
      } else {
        variants.add(v);
      }
    }
    return variants;
  }
}
