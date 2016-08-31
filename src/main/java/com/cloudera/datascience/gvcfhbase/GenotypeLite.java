package com.cloudera.datascience.gvcfhbase;

import java.io.Serializable;

public class GenotypeLite implements Serializable {
  private int sampleIndex;
  private String value;

  public GenotypeLite(int sampleIndex, String value) {
    this.sampleIndex = sampleIndex;
    this.value = value;
  }

  public int getSampleIndex() {
    return sampleIndex;
  }

  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GenotypeLite that = (GenotypeLite) o;

    if (sampleIndex != that.sampleIndex) return false;
    return value.equals(that.value);

  }

  @Override
  public int hashCode() {
    int result = sampleIndex;
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GenotypeLite{" +
        "sampleIndex=" + sampleIndex +
        ", value='" + value + '\'' +
        '}';
  }
}
