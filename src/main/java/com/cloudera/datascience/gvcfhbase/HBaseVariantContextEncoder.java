package com.cloudera.datascience.gvcfhbase;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseVariantContextEncoder extends HBaseVariantEncoder<VariantContext>
    implements Serializable {

  private static final Allele NON_REF = Allele.create("<NON_REF>", false);
  private static final Allele NULL = Allele.create("<NULL>", false);
  
  private SampleNameIndex sampleNameIndex;

  public HBaseVariantContextEncoder(SampleNameIndex sampleNameIndex) {
    this.sampleNameIndex = sampleNameIndex;
  }

  @Override
  public int getNumSamples() {
    return sampleNameIndex.getNumSamples();
  }

  @Override
  public Put encodeVariant(VariantContext variant) {
    // note that we only store one genotype here as we expect to load single sample
    // gvcf files
    Genotype genotype = variant.getGenotype(0);
    int sampleIndex = sampleNameIndex.getSampleIndex(genotype.getSampleName());
    int start = variant.getStart();
    int end = variant.getEnd();
    int keyStart = getKeyStart(variant);
    int keyEnd = getKeyEnd(variant);
    List<Allele> alleles = variant.getAlleles();
    String ref = alleles.get(0).getDisplayString();
    String alt = alleles.get(1).getDisplayString();
    byte[] rowKey = RowKey.toRowKeyBytes(variant.getContig(), keyStart);
    Put put = new Put(rowKey);
    byte[] qualifier = Bytes.toBytes(sampleIndex);
    String val = keyEnd + "," + start + "," + end + "," + ref + "," + alt + "," +
      allelesToString(genotype.getAlleles(), alleles);
    byte[] value = Bytes.toBytes(val);
    put.addColumn(GVCFHBase.SAMPLE_COLUMN_FAMILY, qualifier, value);
    return put;
  }

  private String allelesToString(List<Allele> genotypeAlleles, List<Allele> alleles) {
    StringBuilder sb = new StringBuilder();
    for (Allele a : genotypeAlleles) {
      sb.append(Iterables.indexOf(alleles, a::equals)).append(";");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private List<Allele> allelesFromString(String s, List<Allele> alleles) {
    List<String> strings = Lists.newArrayList(Splitter.on(";").split(s));
    List<Allele> genotypeAlleles = new ArrayList<>();
    for (String index : strings) {
      genotypeAlleles.add(alleles.get(Integer.parseInt(index)));
    }
    return genotypeAlleles;
  }

  @Override
  public VariantContext decodeVariant(RowKey rowKey, Cell cell) {
    int sampleIndex = Bytes.toInt(cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
    String sampleName = sampleNameIndex.getSampleName(sampleIndex);
    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
    String[] splits = val.split(",");
    int keyEnd = Integer.parseInt(splits[0]);
    int start = Integer.parseInt(splits[1]);
    int end = Integer.parseInt(splits[2]);
    String ref = splits[3];
    String alt = splits[4];
    List<Allele> alleles = ImmutableList.of(Allele.create(ref, true), Allele.create(alt));
    List<Allele> genotypeAlleles = allelesFromString(splits[5], alleles);
    Genotype genotype = new GenotypeBuilder(sampleName).alleles(genotypeAlleles).make();
    return newVariantContext(rowKey.contig, start, end, ref, alt, rowKey.pos, keyEnd, genotype);
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
    String keyStartString = (String) variant.getAttribute("KEY_START");
    return keyStartString == null ? variant.getStart() : Integer.parseInt(keyStartString);
  }

  @Override
  public int getKeyEnd(VariantContext variant) {
    String keyEndString = (String) variant.getAttribute("KEY_END");
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

  private static VariantContext newVariantContext(String contig, int start, int end, String ref, String alt,
      int keyStart, int keyEnd, Genotype genotype) {
    VariantContextBuilder builder = new VariantContextBuilder();
    builder.source(HBaseVariantContextEncoder.class.getSimpleName());
    builder.chr(contig);
    builder.start(start);
    builder.stop(end);
    builder.alleles(ref, alt);
    setKeyStartAndEnd(builder, keyStart, keyEnd);
    builder.genotypes(genotype);
    return builder.make();
  }

  private static VariantContextBuilder setKeyStartAndEnd(VariantContextBuilder
      builder, int keyStart, int keyEnd) {
    builder.attribute("KEY_START", Integer.toString(keyStart));
    builder.attribute("KEY_END", Integer.toString(keyEnd));
    return builder;
  }

  @Override
  public List<VariantContext> adjustEnds(List<VariantContext> variantsBySampleIndex,
      int start, int nextKeyEnd) {
    List<VariantContext> variants = new ArrayList<>();
    for (VariantContext v : variantsBySampleIndex) {
      if (Iterables.getFirst(v.getAlternateAlleles(), NULL).equals(NON_REF)) {
        //String ref = start == v.getStart() ? v.getReference().getDisplayString() : "."; // unknown ref (TODO:use fasta)
        String ref = v.getReference().getDisplayString();
        String alt = Iterables.getOnlyElement(v.getAlternateAlleles()).getDisplayString();
        int end = nextKeyEnd < v.getEnd() ? nextKeyEnd : v.getEnd();
        VariantContext variant = newVariantContext(v.getContig(), start, end,
            ref, alt, getKeyStart(v), getKeyEnd(v), v.getGenotype(0));
        variants.add(variant);
      } else {
        variants.add(v);
      }
    }
    return variants;
  }
}
