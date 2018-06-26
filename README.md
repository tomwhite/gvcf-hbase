# Using HBase as a transient GVCF store

Genomic VCF (GVCF) files are like regular VCF files, but they contain a record for every
position in the genome. See [What is a GVCF and how is it different from a 'regular' VCF?](https://software.broadinstitute.org/gatk/guide/article?id=4017).

They are useful for genotyping since they contain information (such as depth information)
even for sites where no variant was detected. GVCF are much bigger than VCFs, however, 
since the number of variants is much smaller than the number of sites. For this reason,
GVCFs often represent a non-variant block of records as a single record, where all the
values are the same (or in the same band).

Combining hundreds of GVCFs is prohibitively slow however, which is why solutions like 
TileDB have arisen. This project allows you to use HBase as an alternative store.

The reason that HBase is a good solution is that it is sparse (so we can take advantage
of a blocking design), it supports fast writes (so we can insert new samples quickly), 
and it is fault-tolerant.

One downside of HBase is that it does not have fast scan speeds, whch means that it is 
not always a good choice for analytics. For combining and processing GVCF files 
however, it is a good fit, and downstream analytics can be performed on other 
storage, such as Parquet or Kudu.

## Requirements

We would like to have a storage layer for GVCF data with the following operations in 
the workflow:

1. For each new sample in the batch, store all positions and variants in the GVCF file 
in HBase.
2. For each variant in the store, read all genotypes for all samples
3. For each position, read all genotypes for all samples

Note that a sample may not have a particular variant, in which case you'll just see the
non-variant records at that position.

## Applications

`CombineGVCFs` is a GATK program that takes a list of GVCF files and combines them into one.
As a proof-of-concept CombineGVCFs was ported to run on the HBase store.

1. GVCF data is written to the store (e.g. by `HaplotypeCaller` programs), possibly in parallel.
2. `CombineGVCFs` is run against the store to produce a `RDD<VariantContext>`, whcih can be written
as a single GVCF file, or passed to later parts of the pipeline (e.g. genotyping).

## Design

### Encoding

Samples are stored in columns in the `s` column family. The column name is an integer 
that represents the zero-based sample index. Both column family and name are chosen to 
be short to make the encoding as small as possible.

Row keys are made up of the following fields:

1. the contig
2. the base pair position

Row keys are encoded (using the HBase [Bytes](https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/util/Bytes.html) class) as follows:

1. the contig (2 characters or less, e.g. "22" or "X") is left padded and converted to two bytes.
2. the base pair position is written as a four-byte integer.

Therefore keys will be sorted by contig, then position within contig.

Each HBase cell contains a variant, represented by an htsjdk `VariantContext` instance.

### Splits

Tables in HBase are split into regions. This can be done automatically, but the design
here relies on the tables being pre-split. Split points are chosen every N base pair 
positions, where N is chosen before loading a batch, and must be the same for every 
sample loaded in a batch.

It must be possible to find the variant at every location in a split, without reading
the preceding or following split. This allows each split to be processed independently
of the others. To achieve this, extra rows are added as follows:

1. Break `NON_REF` blocks that cross splits.
2. Add "no call header spacers" if there is no variant at the start of the split.
3. Add "no call" at the start of any run of locations with no variant information.

For example, to illustrate 1 suppose N=4, and the following is the GVCF file for
the first sample (some fields have been dropped for simplicity):

```
#CHROM POS REF ALT INFO SAMPLE
1 1 A G               0/1
1 2 G <NON_REF> END=7 -  
1 8 G C               1/1
```

The block from 2 to 7 will be split into two when it is stored in HBase, since it 
overlaps the 4 split point. Conceptually, this looks like this:

```
Split 1 | 1-1 A:G
        | 2-4 G:<NON_REF>
Split 2 | 5-7 G:<NON_REF>
        | 8-8 G:C
```

This allows the process reading the second split (which 
starts at position 5) to access information at positions 5, 6, and 7 in the block, 
which would not have been possible otherwise if the block has not been split, since the
record would have been only readable from the previous partition.

# Building

```
mvn install -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dhttps.protocols="TLSv1.2"
```