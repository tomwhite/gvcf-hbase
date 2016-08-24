package com.cloudera.datascience.gvcfhbase;

import java.io.Serializable;

public class GenotypeLite implements Serializable {
  private String sampleName;
  private String value;

  public GenotypeLite(String sampleName, String value) {
    this.sampleName = sampleName;
    this.value = value;
  }

  public String getSampleName() {
    return sampleName;
  }

  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GenotypeLite that = (GenotypeLite) o;

    if (!sampleName.equals(that.sampleName)) return false;
    return value.equals(that.value);

  }

  @Override
  public int hashCode() {
    int result = sampleName.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GenotypeLite{" +
        "sampleName='" + sampleName + '\'' +
        ", value='" + value + '\'' +
        '}';
  }
}
