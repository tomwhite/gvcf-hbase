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
2. For each variant in the store, read all genotypes for all samples
3. For each position, read all genotypes for all samples

Note that a sample may not have a particular variant, in which case you'll just see the
non-variant records at that position.

## Design

### Encoding

Samples are stored in columns in the `s` column family. The column name is an integer 
that represents the zero-based sample index. Both column family and name are chosen to 
be short to make the encoding as small as possible.

Row keys are made up of the following fields:

1. the contig
2. the base pair position
3. the reference allele
4. the alternate allele

[TODO: describe how row keys are encoded]

### Splits

Tables in HBase are split into regions. This can be done automatically, but this design
relies on the tables being pre-split. Split points are chosen every N base pair 
positions, where N is chosen before loading a batch, and must be the same for every 
sample loaded in a batch.

For example, suppose N=4, and the following is the GVCF file for the first sample (some
fields have been dropped for simplicity):

```
#CHROM POS REF ALT INFO SAMPLE
1 1 A G               0|0
1 2 G <NON_REF> END=7 -  
1 8 T C               1|1
```

The block from 2 to 7 will be split into two when it is stored in HBase, since it 
overlaps the 4 split point. This allows the process reading the second split (which 
starts at position 5) to access information at positions 5, 6, and 7 in the block, 
which would not have been possible otherwise if the block has not been split, since the
record would have been only readable from the previous partition.