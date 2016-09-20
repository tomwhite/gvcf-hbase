package com.cloudera.datascience.gvcfhbase;

import htsjdk.samtools.util.Locatable;
import java.io.Serializable;

/**
 * An interface implemented by clients to combine variants at the same genomic location.
 * Implementations may be stateful, in order to perform sophisticated merging.
 */
public interface VariantCombiner<V, T> extends Serializable {
  /**
   * Takes a genomic range (where the start is the start position for all the variants
   * in the collection, and the end is the position before the start position of the next
   * row) and a collection of variants.
   * The return value is zero or more combined values (e.g.
   * {@link htsjdk.variant.variantcontext.VariantContext}).
   * @param location the range of loci to consider
   * @param variants the variants that start at the start of the location
   * @return zero or more combined values
   */
  Iterable<T> combine(Locatable location, Iterable<V> variants);

  /**
   * Call to indicate that there are no more rows, so return any final values.
   * @return zero or more combined values
   */
  Iterable<T> finish();
}
