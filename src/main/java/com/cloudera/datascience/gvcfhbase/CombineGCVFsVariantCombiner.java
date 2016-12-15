package com.cloudera.datascience.gvcfhbase;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import java.util.List;

public class CombineGCVFsVariantCombiner implements VariantCombiner<VariantContext, VariantContext> {
  @Override
  public Iterable<VariantContext> combine(Locatable loc, Iterable<VariantContext> v) {
    List<VariantContext> variants = Lists.newArrayList(v);
    VariantContext firstVariant = variants.get(0);
    if (firstVariant.getStart() != loc.getStart()) { // ignore fake variant from split
      return ImmutableList.of();
    }
    VariantContextBuilder builder = new VariantContextBuilder();
    builder.source(getClass().getSimpleName());
    builder.chr(firstVariant.getContig());
    builder.start(firstVariant.getStart());
    builder.stop(firstVariant.getEnd());
    builder.alleles(firstVariant.getAlleles());
    List<Genotype> genotypes = Lists.newArrayList(firstVariant.getGenotypes());
    for (int i = 1; i < variants.size(); i++) {
      genotypes.addAll(variants.get(i).getGenotypes());
    }
    builder.genotypes(genotypes);
    return ImmutableList.of(builder.make());
  }

  @Override
  public Iterable<VariantContext> finish() {
    return ImmutableList.of();
  }
}
