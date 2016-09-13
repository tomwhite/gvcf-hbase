package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.List;

public class VariantLite implements Serializable {
  private String contig;
  private int start;
  private int end;
  private int keyStart;
  private int keyEnd;
  private List<GenotypeLite> genotypes;

  public VariantLite(String contig,int start, int end, GenotypeLite genotype) {
    this(contig, start, end, start, end, ImmutableList.of(genotype));
  }

  public VariantLite(String contig, int start, int end, int keyStart, int
      keyEnd, GenotypeLite genotype) {
    this(contig, start, end, keyStart, keyEnd, ImmutableList.of(genotype));
  }

  private VariantLite(String contig, int start, int end, int keyStart, int keyEnd,
      List<GenotypeLite> genotypes) {
    this.contig = contig;
    this.start = start;
    this.end = end;
    this.keyStart = keyStart;
    this.keyEnd = keyEnd;
    this.genotypes = genotypes;
  }

  public String getContig() {
    return contig;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public int getKeyStart() {
    return keyStart;
  }

  public int getKeyEnd() {
    return keyEnd;
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
    if (keyStart != that.keyStart) return false;
    if (keyEnd != that.keyEnd) return false;
    if (contig != null ? !contig.equals(that.contig) : that.contig != null) return false;
    return genotypes != null ? genotypes.equals(that.genotypes) : that.genotypes == null;

  }

  @Override
  public int hashCode() {
    int result = contig != null ? contig.hashCode() : 0;
    result = 31 * result + start;
    result = 31 * result + end;
    result = 31 * result + keyStart;
    result = 31 * result + keyEnd;
    result = 31 * result + (genotypes != null ? genotypes.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "VariantLite{" +
        "contig='" + contig + '\'' +
        ", start=" + start +
        ", end=" + end +
        ", keyStart=" + keyStart +
        ", keyEnd=" + keyEnd +
        ", genotypes=" + genotypes +
        '}';
  }
}