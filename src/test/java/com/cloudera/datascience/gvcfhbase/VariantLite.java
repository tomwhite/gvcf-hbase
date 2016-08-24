package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.List;

public class VariantLite implements Serializable {
  private int start;
  private int end;
  private List<GenotypeLite> genotypes;

  public VariantLite(int start, int end, GenotypeLite genotype) {
    this(start, end, ImmutableList.of(genotype));
  }

  public VariantLite(int start, int end, List<GenotypeLite> genotypes) {
    this.start = start;
    this.end = end;
    this.genotypes = genotypes;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public List<GenotypeLite> getGenotypes() {
    return genotypes;
  }

  public GenotypeLite getGenotype() {
    return Iterables.getOnlyElement(genotypes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    VariantLite that = (VariantLite) o;

    if (start != that.start) return false;
    if (end != that.end) return false;
    return genotypes.equals(that.genotypes);

  }

  @Override
  public int hashCode() {
    int result = start;
    result = 31 * result + end;
    result = 31 * result + genotypes.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "VariantLite{" +
        "start=" + start +
        ", end=" + end +
        ", genotypes=" + genotypes +
        '}';
  }
}
