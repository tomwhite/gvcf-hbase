package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.List;

public class VariantLite implements Serializable {
  private String contig;
  private int start;
  private int end;
  private String ref;
  private String alt;
  private int keyStart;
  private int keyEnd;
  private List<GenotypeLite> genotypes;

  public VariantLite(String contig, int start, int end, String ref, String alt,
      GenotypeLite genotype) {
    this(contig, start, end, ref, alt, start, end, ImmutableList.of(genotype));
  }

  public VariantLite(String contig, int start, int end, String ref, String alt,
      int keyStart, int keyEnd, GenotypeLite genotype) {
    this(contig, start, end, ref, alt, keyStart, keyEnd, ImmutableList.of(genotype));
  }

  private VariantLite(String contig, int start, int end, String ref, String alt,
      int keyStart, int keyEnd, List<GenotypeLite> genotypes) {
    this.contig = contig;
    this.start = start;
    this.ref = ref;
    this.alt = alt;
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

  public String getRef() {
    return ref;
  }

  public String getAlt() {
    return alt;
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
  public String toString() {
    return "VariantLite{" +
        "contig='" + contig + '\'' +
        ", start=" + start +
        ", end=" + end +
        ", ref='" + ref + '\'' +
        ", alt='" + alt + '\'' +
        ", keyStart=" + keyStart +
        ", keyEnd=" + keyEnd +
        ", genotypes=" + genotypes +
        '}';
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
    if (!contig.equals(that.contig)) return false;
    if (!ref.equals(that.ref)) return false;
    if (alt != null ? !alt.equals(that.alt) : that.alt != null) return false;
    return genotypes.equals(that.genotypes);

  }

  @Override
  public int hashCode() {
    int result = contig.hashCode();
    result = 31 * result + start;
    result = 31 * result + end;
    result = 31 * result + ref.hashCode();
    result = 31 * result + (alt != null ? alt.hashCode() : 0);
    result = 31 * result + keyStart;
    result = 31 * result + keyEnd;
    result = 31 * result + genotypes.hashCode();
    return result;
  }
}
