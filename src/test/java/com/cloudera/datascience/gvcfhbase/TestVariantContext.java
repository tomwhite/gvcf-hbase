package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.Iterators;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TestVariantContext {

  @Test
  public void test() {
    VCFFileReader reader = new VCFFileReader(new File("src/test/resources/g.vcf"), false);
    List<VariantContext> variants = new ArrayList<>();
    Iterators.addAll(variants, reader.iterator());
    System.out.println(variants);
    for (int i = 0; i < 3; i++) {
      VariantContext variantContext = variants.get(i);
      System.out.println("start=" + variantContext.getStart());
      System.out.println("end=" + variantContext.getEnd());
      System.out.println("alt=" + variantContext.getAlternateAlleles());
      VariantContextBuilder builder = new VariantContextBuilder(variantContext);
      builder.attribute("tom", "1");
      VariantContext make = builder.make();
      System.out.println(make);
    }

  }
}
