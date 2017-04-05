package com.cloudera.datascience.gvcfhbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.engine.ReferenceDataSource;
import org.broadinstitute.hellbender.engine.ReferenceFileSource;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;

/**
 * This is a HBase and Spark verion of GATK3's CombineGCVFs.
 * It is untested, and actually is probably a long way off working. The basic structure
 * of the code is basically right. There are probably lots of edge cases to get right
 * around partition boundaries in HBase. There are also issues around licensing and
 * getting test cases. Finally, this was a speculative project - there is currently no
 * user demand for this feature.
 */
public class CombineGCVFs {

  public static JavaRDD<VariantContext> combine(HBaseVariantEncoder<VariantContext> variantEncoder,
      TableName tableName, JavaHBaseContext hbaseContext, String referencePath) {
    // naively, one would think that we could use Spark aggregate here, but the way
    // that GATK3 MR framework works is that there is a single reduce object
    // (OverallState) since the framework runs on one machine (multi-threaded)
    // so in Spark we can't do that since there is more than one reducer running.

    // What we can do is run a mapPartitions, and do the aggregation in there, so there
    // is one OverallState per partition.
    return GVCFHBase.load(variantEncoder, tableName, hbaseContext,
        new CombineGCVFsVariantCombiner(referencePath));
  }

  private static class PositionalState {
    final List<VariantContext> VCs;
    final Set<String> samples = new HashSet<>();
    final byte[] refBases;
    final Locatable loc;
    public PositionalState(final List<VariantContext> VCs, final byte[] refBases, final Locatable loc) {
      this.VCs = VCs;
      for(final VariantContext vc : VCs){
        samples.addAll(vc.getSampleNames());
      }
      this.refBases = refBases;
      this.loc = loc;
    }
  }

  private static class OverallState implements Serializable {
    final LinkedList<VariantContext> VCs = new LinkedList<>();
    final Set<String> samples = new HashSet<>();
    Locatable prevPos = null;
    byte refAfterPrevPos;

    public OverallState() {}
  }

  public static class CombineGCVFsVariantCombiner implements VariantCombiner<VariantContext, VariantContext> {

    private boolean USE_BP_RESOLUTION = false;

    private int multipleAtWhichToBreakBands = 0;

    private final OverallState overallState = new OverallState();
    private String referencePath;

    public CombineGCVFsVariantCombiner(String referencePath) {
      this.referencePath = referencePath;
    }

    @Override
    public Iterable<VariantContext> combine(Locatable loc, Iterable<VariantContext> v) {
      ReferenceDataSource referenceDataSource = new ReferenceFileSource(new File(referencePath)); // TODO: load ref from HDFS or nio path
      ReferenceSequence ref = referenceDataSource.queryAndPrefetch(loc.getContig(), loc.getStart(), loc.getStart() + 10000);// TODO: get full reference for the partition
      PositionalState positionalState = new PositionalState(Lists.newArrayList(v),
          ref.getBases(), loc);
      return reduce(positionalState, overallState);
    }

    private Iterable<VariantContext> reduce(final PositionalState startingStates, final OverallState
        previousState) {
      // like GATK3 implementation, which updates previousState, and returns 0 or more
      // variants to write out

      if ( startingStates == null )
        return ImmutableList.of();

      List<VariantContext> ret = Lists.newArrayList();

      if ( !startingStates.VCs.isEmpty() ) {
        if ( ! okayToSkipThisSite(startingStates, previousState) ) {
          Iterable<VariantContext> val = endPreviousStates(previousState, new Interval
                  (startingStates.loc
                      .getContig(),
                      startingStates.loc.getStart() - 1, startingStates.loc.getEnd() - 1),
              startingStates, false);
          Iterables.addAll(ret, val);
        }
        previousState.VCs.addAll(startingStates.VCs);
        for(final VariantContext vc : previousState.VCs){
          previousState.samples.addAll(vc.getSampleNames());
        }

      }

      if ( breakBand(startingStates.loc) || containsEndingContext(previousState.VCs, startingStates.loc.getStart()) ) {
        Iterable<VariantContext> val = endPreviousStates(previousState, startingStates
            .loc, startingStates, true);
        Iterables.addAll(ret, val);
      }
      return ret;
    }


    /**
     * Is it okay to skip the given position?
     *
     * @param startingStates  state information for this position
     * @param previousState   state information for the last position for which we created a VariantContext
     * @return true if it is okay to skip this position, false otherwise
     */
    private boolean okayToSkipThisSite(final PositionalState startingStates, final OverallState previousState) {
      final int thisPos = startingStates.loc.getStart();
      final Locatable lastPosRun = previousState.prevPos;
      Set<String> intersection = new HashSet<String>(startingStates.samples);
      intersection.retainAll(previousState.samples);

      //if there's a starting VC with a sample that's already in a current VC, don't skip this position
      return lastPosRun != null && thisPos == lastPosRun.getStart() + 1 && intersection.isEmpty();
    }

    /**
     * Should we break bands at the given position?
     *
     * @param loc  the genomic location to evaluate against
     *
     * @return true if we should ensure that bands should be broken at the given position, false otherwise
     */
    private boolean breakBand(final Locatable loc) {
      return USE_BP_RESOLUTION ||
          (loc != null && multipleAtWhichToBreakBands > 0 && (loc.getStart()+1) % multipleAtWhichToBreakBands == 0);  // add +1 to the loc because we want to break BEFORE this base
    }

    /**
     * Does the given list of VariantContexts contain any whose context ends at the given position?
     *
     * @param VCs  list of VariantContexts
     * @param pos  the position to check against
     * @return true if there are one or more VCs that end at pos, false otherwise
     */
    private boolean containsEndingContext(final List<VariantContext> VCs, final int pos) {
      if ( VCs == null ) throw new IllegalArgumentException("The list of VariantContexts cannot be null");

      for ( final VariantContext vc : VCs ) {
        if ( isEndingContext(vc, pos) )
          return true;
      }
      return false;
    }

    /**
     * Does the given variant context end (in terms of reference blocks, not necessarily formally) at the given position.
     * Note that for the purposes of this method/tool, deletions are considered to be single base events (as opposed to
     * reference blocks), hence the check for the number of alleles (because we know there will always be a <NON_REF> allele).
     *
     * @param vc   the variant context
     * @param pos  the position to query against
     * @return true if this variant context "ends" at this position, false otherwise
     */
    private boolean isEndingContext(final VariantContext vc, final int pos) {
      return vc.getNAlleles() > 2 || vc.getEnd() == pos;
    }

    /**
     * Disrupt the VariantContexts so that they all stop at the given pos, write them out, and put the remainder back in the list.
     * @param state   the previous state with list of active VariantContexts
     * @param pos   the position for the starting VCs
     * @param startingStates the state for the starting VCs
     * @param atCurrentPosition  indicates whether we output a variant at the current position, independent of VCF start/end, i.e. in BP resolution mode
     */
    private Iterable<VariantContext> endPreviousStates(final OverallState state, final Locatable pos, final
    PositionalState startingStates, boolean atCurrentPosition) {
      List<VariantContext> ret = Lists.newArrayList();

      final byte refBase = startingStates.refBases[0];
      //if we're in BP resolution mode or a VC ends at the current position then the reference for the next output VC (refNextBase)
      // will be advanced one base
      final byte refNextBase = (atCurrentPosition) ? (startingStates.refBases.length > 1 ? startingStates.refBases[1] : (byte)'N' ): refBase;

      final List<VariantContext> stoppedVCs = new ArrayList<>(state.VCs.size());

      for ( int i = state.VCs.size() - 1; i >= 0; i-- ) {
        final VariantContext vc = state.VCs.get(i);
        //the VC for the previous state will be stopped if its position is previous to the current position or it we've moved to a new contig
        if ( vc.getStart() <= pos.getStart() || !vc.getChr().equals(pos.getContig())) {

          stoppedVCs.add(vc);

          // if it was ending anyways, then remove it from the future state
          if ( vc.getEnd() == pos.getStart()) {
            state.samples.removeAll(vc.getSampleNames());
            state.VCs.remove(i);
            continue; //don't try to remove twice
          }

          //if ending vc is the same sample as a starting VC, then remove it from the future state
          if(startingStates.VCs.size() > 0 && !atCurrentPosition && startingStates.samples.containsAll(vc.getSampleNames())) {
            state.samples.removeAll(vc.getSampleNames());
            state.VCs.remove(i);
          }
        }
      }

      //output the stopped VCs if there is no previous output (state.prevPos == null) or our current position is past
      // the last write position (state.prevPos)
      //NOTE: BP resolution with have current position == state.prevPos because it gets output via a different control flow
      if ( !stoppedVCs.isEmpty() &&  (state.prevPos == null || isPast(pos, state.prevPos)
      )) {
        final Locatable gLoc = new Interval(stoppedVCs.get(0).getChr(), pos.getStart(), pos.getStart());

        // we need the specialized merge if the site contains anything other than ref blocks
        final VariantContext mergedVC;
        if ( containsTrueAltAllele(stoppedVCs) ) {
          // TODO: make instance variable (but need to make serializable)
          ReferenceConfidenceVariantContextMerger merger = new
              ReferenceConfidenceVariantContextMerger();
          mergedVC = merger.merge(stoppedVCs, gLoc, refBase, false, false);
        } else {
          mergedVC = referenceBlockMerge(stoppedVCs, state, pos.getStart());
        }

        ret.add(mergedVC);
        state.prevPos = gLoc;
        state.refAfterPrevPos = refNextBase;
      }
      return ret;
    }

    /**
     * Combine a list of reference block VariantContexts.
     * We can't use GATKVariantContextUtils.simpleMerge() because it is just too slow for this sort of thing.
     *
     * @param VCs   the variant contexts to merge
     * @param state the state object
     * @param end   the end of this block (inclusive)
     * @return a new merged VariantContext
     */
    private VariantContext referenceBlockMerge(final List<VariantContext> VCs, final OverallState state, final int end) {

      final VariantContext first = VCs.get(0);

      // ref allele and start
      final Allele refAllele;
      final int start;
      if ( state.prevPos == null || !state.prevPos.getContig().equals(first.getChr()) || first.getStart() >= state.prevPos.getStart() + 1) {
        start = first.getStart();
        refAllele = first.getReference();
      } else {
        start = state.prevPos.getStart() + 1;
        refAllele = Allele.create(state.refAfterPrevPos, true);
      }

      // attributes
      final Map<String, Object> attrs = new HashMap<>(1);
      if ( !USE_BP_RESOLUTION && end != start )
        attrs.put(VCFConstants.END_KEY, Integer.toString(end));

      // genotypes
      final GenotypesContext genotypes = GenotypesContext.create();
      for ( final VariantContext vc : VCs ) {
        for ( final Genotype g : vc.getGenotypes() )
          genotypes.add(new GenotypeBuilder(g).alleles(GATKVariantContextUtils.noCallAlleles(g.getPloidy())).make());
      }

      return new VariantContextBuilder("", first.getChr(), start, end, Arrays.asList(refAllele, GATKVCFConstants.NON_REF_SYMBOLIC_ALLELE)).attributes(attrs).genotypes(genotypes).make();
    }

    /**
     * Does the given list of VariantContexts contain any with an alternate allele other than <NON_REF>?
     *
     * @param VCs  list of VariantContexts
     * @return true if there are one or more VCs that contain a true alternate allele, false otherwise
     */
    private boolean containsTrueAltAllele(final List<VariantContext> VCs) {
      if ( VCs == null ) throw new IllegalArgumentException("The list of VariantContexts cannot be null");

      for ( final VariantContext vc : VCs ) {
        if ( vc.getNAlleles() > 2 )
          return true;
      }
      return false;
    }

    @Override
    public Iterable<VariantContext> finish() {
      // there shouldn't be any state left unless the user cut in the middle of a gVCF block
      if (!overallState.VCs.isEmpty()) {
        System.err.println("You have asked for an interval that cuts in the middle of " +
            "one or more gVCF " +
            "blocks. Please note that this will cause you to lose records that don't end within your interval.");
      }
      return ImmutableList.of();
    }
  }

  /**
   * Based on logic in GenomeLoc#isPast
   */
  private static boolean isPast(Locatable pos1, Locatable pos2) {
    // TODO: reconcile with RowKey
    byte[] contig1Bytes = Bytes.toBytes(StringUtils.leftPad(pos1.getContig(), 2));
    byte[] contig2Bytes = Bytes.toBytes(StringUtils.leftPad(pos2.getContig(), 2));
    int comparison = Bytes.compareTo(contig1Bytes, contig2Bytes);
    return ( comparison == 1 || ( comparison == 0 && pos1.getStart() > pos2.getEnd() ));
  }
}
