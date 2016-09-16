package com.cloudera.datascience.gvcfhbase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A global map between sample names and IDs.
 */
public class SampleNameIndex implements Serializable {

  private List<String> sampleNames;
  private Map<String, Integer> sampleNamesToIndexes;

  public SampleNameIndex(List<String> sampleNames) {
    this.sampleNames = sampleNames;
    this.sampleNamesToIndexes = new HashMap<>(sampleNames.size());
    for (int i = 0 ; i < sampleNames.size(); i++) {
      sampleNamesToIndexes.put(sampleNames.get(i), i);
    }
  }

  public int getNumSamples() {
    return sampleNames.size();
  }

  public String getSampleName(int sampleIndex) {
    return sampleNames.get(sampleIndex);
  }

  public int getSampleIndex(String sampleName) {
    return sampleNamesToIndexes.get(sampleName);
  }
}
