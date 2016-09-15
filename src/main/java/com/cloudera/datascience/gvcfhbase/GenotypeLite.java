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

}
