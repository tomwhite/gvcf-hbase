package com.cloudera.datascience.gvcfhbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFHeader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.seqdoop.hadoop_bam.LazyVCFGenotypesContext;
import org.seqdoop.hadoop_bam.VariantContextCodec;
import org.seqdoop.hadoop_bam.VariantContextWithHeader;

public class HBaseVariantContextEncoder extends HBaseVariantEncoder<VariantContext>
    implements Serializable {

  public static final String KEY_START = "KEY_START";
  public static final String KEY_END = "KEY_END";

  private final SampleNameIndex sampleNameIndex;
  private final Map<String, VCFHeader> perSampleHaders;

  public HBaseVariantContextEncoder(SampleNameIndex sampleNameIndex, VCFHeader vcfHeader) {
    this.sampleNameIndex = sampleNameIndex;
    perSampleHaders = Maps.newLinkedHashMap();
    for (String sampleName : sampleNameIndex.getSampleNames()) {
      perSampleHaders.put(sampleName,
          new VCFHeader(vcfHeader.getMetaDataInInputOrder(), ImmutableList.of(sampleName)));
    }
  }

  @Override
  public int getNumSamples() {
    return sampleNameIndex.getNumSamples();
  }

  @Override
  public Put encodeVariant(VariantContext variant) throws IOException {
    // note that we only store one genotype here as we expect to load single sample
    // gvcf files
    Preconditions.checkArgument(variant.getNSamples() == 1);
    Genotype genotype = variant.getGenotype(0);
    String sampleName = genotype.getSampleName();
    int sampleIndex = sampleNameIndex.getSampleIndex(sampleName);
    int keyStart = getKeyStart(variant);
    byte[] rowKey = RowKey.toRowKeyBytes(variant.getContig(), keyStart);
    Put put = new Put(rowKey);
    byte[] qualifier = Bytes.toBytes(sampleIndex);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    VariantContextCodec.write(out, new VariantContextWithHeader(variant, perSampleHaders.get(sampleName)));
    put.addColumn(GVCFHBase.SAMPLE_COLUMN_FAMILY, qualifier, baos.toByteArray());
    return put;
  }

  @Override
  public VariantContext decodeVariant(RowKey rowKey, Cell cell, boolean
      includeKeyAttributes) throws IOException {

    int sampleIndex = Bytes.toInt(cell.getQualifierArray(), cell.getQualifierOffset());
    String sampleName = sampleNameIndex.getSampleName(sampleIndex);

    ByteArrayInputStream bais = new ByteArrayInputStream(cell
        .getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
    DataInputStream in = new DataInputStream(bais);
    VariantContext variant = VariantContextCodec.read(in);
    // reify genotypes by building a new object that doesn't have lazy genotypes
    VariantContextBuilder builder = new VariantContextBuilder(variant);
    if (!includeKeyAttributes) {
      builder.rmAttributes(ImmutableList.of(KEY_START, KEY_END));
    }
    GenotypesContext genotypes = variant.getGenotypes();
    Preconditions.checkArgument(variant.getNSamples() == 1);
    LazyVCFGenotypesContext.HeaderDataCache headerDataCache = new LazyVCFGenotypesContext.HeaderDataCache();
    headerDataCache.setHeader(perSampleHaders.get(sampleName));
    ((LazyVCFGenotypesContext) genotypes).getParser().setHeaderDataCache(headerDataCache);
    builder.genotypes(genotypes);

    return builder.make();
  }

  @Override
  public int getSampleIndex(VariantContext variant) {
    return sampleNameIndex.getSampleIndex(variant.getGenotype(0).getSampleName());
  }

  @Override
  public int getStart(VariantContext variant) {
    return variant.getStart();
  }

  @Override
  public int getEnd(VariantContext variant) {
    return variant.getEnd();
  }

  public int getKeyStart(VariantContext variant) {
    String keyStartString = (String) variant.getAttribute(KEY_START);
    return keyStartString == null ? variant.getStart() : Integer.parseInt(keyStartString);
  }

  @Override
  public int getKeyEnd(VariantContext variant) {
    String keyEndString = (String) variant.getAttribute(KEY_END);
    return keyEndString == null ? variant.getEnd() : Integer.parseInt(keyEndString);
  }

  @Override
  public VariantContext[] split(VariantContext variant, int key1End, int key2Start) {
    int start = variant.getStart();
    int end = variant.getEnd();
    return new VariantContext[] {
        setKeyStartAndEnd(new VariantContextBuilder(variant), start, key1End).make(),
        setKeyStartAndEnd(new VariantContextBuilder(variant), key2Start, end).make()
    };
  }

  private static VariantContextBuilder setKeyStartAndEnd(VariantContextBuilder
      builder, int keyStart, int keyEnd) {
    builder.attribute(KEY_START, Integer.toString(keyStart));
    builder.attribute(KEY_END, Integer.toString(keyEnd));
    return builder;
  }
}
