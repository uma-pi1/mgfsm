package de.mpii.fsm.mgfsm;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import de.mpii.fsm.driver.FsmConfig.IndexingMethod;
import de.mpii.fsm.mgfsm.encoders.BaseGapEncoder;
import de.mpii.fsm.mgfsm.encoders.SimpleGapEncoder;
import de.mpii.fsm.mgfsm.encoders.SplitGapEncoder;
import de.mpii.fsm.util.IntArrayWritable;
import de.mpii.fsm.util.PrimitiveUtils;



/** Mapper reads (identifier,transaction) pairs and emits (sequence, 1) pairs. For each item
 * contained in the input transaction, we retrieve its partition and emit the encoded transaction
 * using the partition id concatenated with the sequence as key. Even if the sequence contains more
 * than one item from the same partition it is emitted only once per partition
 * 
 * @param key transaction id
 * @param value transaction */
class FsmMapper extends Mapper<LongWritable, IntArrayWritable, BytesWritable, IntWritable> {

  /** an index mapping items to partitions */
  Map<Integer, Integer> itemToPartition;

  /** an index mapping partitions to their set of items */
  Map<Integer, TreeSet<Integer>> partitionToItems;

  /** a set containing the partitions for which the current transaction has already been emitted
   * --used to avoid double emissions */
  HashSet<Integer> partitionsOfTransaction = new HashSet<Integer>();

  /** Mapping of partition ids to their item id range ({@see PrimitiveUtils#combine(int, int)} ). */
  long[] partitionToPivotRange;

  /** The minmax transaction index (used if {@link FsmJob#INDEXING_METHOD} is set to
   * {@link IndexingMethod#MINMAX}). Maps partition id to the first and last occurrance of an item
   * of the respective partition in the transactions ({@see PrimitiveUtils#combine(int, int)}). */
  long[] partitionIndexMinMax;

  /** The full transaction index (used if {@link FsmJob#INDEXING_METHOD} is set to
   * {@link IndexingMethod#FULL}). Maps partition id to a position in the arrays
   * {@link #partitionIndexFullOffset} (representing an offset) and {@link #partitionIndexFullNext}
   * (representing next position for this partition id or -1 if none) */
  // TODO: we could potentially optimize this index for cases in which the input contains
  // large gaps (by exploiting the logical position of each item)
  int[] partitionIndexFullTail;

  int[] partitionIndexFullHead;

  int[] partitionIndexFullNext;

  int[] partitionIndexFullOffset;

  int partitionIndexFullSize;

  // IndexingMethod INDEXING_METHOD = Config.INDEXING_METHOD;

  /** counter value used for each emitted sequence */
  IntWritable supportWrapper = new IntWritable(1);

  /** encoder of an output transaction used when we do not split transactions (see ALLOW_SPLITS) */
  SimpleGapEncoder simpleEncoder;

  /** encoder of an output transaction used when we split transactions (see ALLOW_SPLITS) */
  SplitGapEncoder splitEncoder;

  /** if positive, only the respective partition is created -- for debugging reasons */
  int debugPartId;

  int gamma;

  int lambda;

  int neighborhoodSize;

  boolean COMPRESS_GAPS;

  @SuppressWarnings("unchecked")
  protected void setup(Context context) throws IOException, InterruptedException {  
	  

    FsmJob.getCommonConfig().setAllowSplits( context.getConfiguration().getBoolean(
        "org.apache.mahout.fsm.partitioning.allowSplits", FsmJob.getCommonConfig().isAllowSplits()));

    FsmJob.getCommonConfig().setIndexingMethod(context.getConfiguration().getEnum(
        "org.apache.mahout.fsm.partitioning.indexingMethod", FsmJob.getCommonConfig().getIndexingMethod()));

    FsmJob.getCommonConfig().setRemoveUnreachable(context.getConfiguration().getBoolean(
        "org.apache.mahout.fsm.partitioning.removeUnreachable", FsmJob.getCommonConfig().isRemoveUnreachable()));

    COMPRESS_GAPS = context.getConfiguration().getBoolean("org.apache.mahout.fsm.partitioning.compressGaps",
        FsmJob.getCommonConfig().isCompressGaps());

    // read dictionary, map from partitions to item identifiers, and
    // reverse map from distributed cache
    try {
      ObjectInputStream is = new ObjectInputStream(new FileInputStream("itemToPartition"));
      itemToPartition = (Map<Integer, Integer>) is.readObject();
      is.close();
      is = new ObjectInputStream(new FileInputStream("partitionToItems"));
      partitionToItems = (HashMap<Integer, TreeSet<Integer>>) is.readObject();
      is.close();
    } catch (IOException e) {
      FsmJob.LOGGER.severe("Reducer: error reading from dCache: " + e);
    } catch (ClassNotFoundException e) {
      FsmJob.LOGGER.severe("Mapper error during deserialization: " + e);
    }

    // initialize encoders and other parameters
    int totalPartitions = context.getConfiguration().getInt(
        "org.apache.mahout.fsm.partitioning.totalPartitions", 0);
    gamma = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.gamma", -1);
    lambda = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.lambda", -1);
    neighborhoodSize = (gamma + 1) * (lambda - 1);
    debugPartId = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.debugPartId", -1);
    simpleEncoder = new SimpleGapEncoder(gamma, lambda, new BytesWritable(), COMPRESS_GAPS,
        FsmJob.getCommonConfig().isRemoveUnreachable());
    splitEncoder = new SplitGapEncoder(gamma, lambda, new ArrayList<BytesWritable>(),
        COMPRESS_GAPS, FsmJob.getCommonConfig().isRemoveUnreachable());
    partitionToPivotRange = new long[totalPartitions + 1];
    switch (FsmJob.getCommonConfig().getIndexingMethod()) {
    case NONE:
      partitionIndexMinMax = null;
      partitionIndexFullHead = null;
      partitionIndexFullTail = null;
      partitionIndexFullNext = null;
      partitionIndexFullOffset = null;
      break;
    case MINMAX:
      partitionIndexMinMax = new long[totalPartitions + 1];
      partitionIndexFullHead = null;
      partitionIndexFullTail = null;
      partitionIndexFullNext = null;
      partitionIndexFullOffset = null;
      break;
    case FULL:
      partitionIndexMinMax = null;
      partitionIndexFullHead = new int[totalPartitions + 1];
      partitionIndexFullTail = new int[totalPartitions + 1];
      partitionIndexFullNext = new int[100];
      partitionIndexFullOffset = new int[100];
      break;
    default:
      // should not enter here
      break;
    }
  }

  protected void map(LongWritable key, IntArrayWritable value, Context context)
      throws IOException, InterruptedException {

	  
    // whether to encode transaction exploiting partition ordering or
    // emit it as is -- for debugging reasons
    final boolean EXPLOIT_ORDERING = true;

    // clear partition set for this transaction
    partitionsOfTransaction.clear();

    // for each transaction item, retrieve its partition and encode it
    // using this partition
    // emit the encoded sequence as key and count 1 as value
    int[] transaction = value.getContents();

    // make sure index data structures are large enough
    partitionIndexFullSize = 0;
    if (FsmJob.getCommonConfig().getIndexingMethod().equals(IndexingMethod.FULL)
        && transaction.length > partitionIndexFullNext.length) {
      partitionIndexFullNext = new int[2 * transaction.length]; // overprovision
      partitionIndexFullOffset = new int[2 * transaction.length];
    }

    // the pivot range [beginItem, endItem] of the partition, packed in
    // a long value
    int beginItem;
    int endItem;
    long pivotRange;

    // iterate through transaction, if no index is used perform encoding
    // at the same time
    for (int offset = 0; offset < transaction.length; offset++) {
      int item = transaction[offset];

      // retrieve the partition of the item, infrequent and stop words
      // have no partition (null)
      if (itemToPartition.get(item) == null) {
        continue;
      }
      int partitionId = itemToPartition.get(item);

      // only output this specific partition -- for debugging reasons
      if (debugPartId > 0 && partitionId != debugPartId) {
        continue;
      }

      // whether this partition has been processed before for this
      // transaction
      boolean isFirstOccurrence = partitionsOfTransaction.add(partitionId);

      // if we have processed this partition before and unless we need
      // to build a partition index
      // we continue with the next item
      if (FsmJob.getCommonConfig().getIndexingMethod().equals(IndexingMethod.NONE) && !isFirstOccurrence) {
        continue;
      }

      // retrieve the pivot range [beginItem, endItem] of this
      // partition
      if (partitionToPivotRange[partitionId] != 0L) {
        // we have already indexed this range, retrieve it
        pivotRange = partitionToPivotRange[partitionId];
        beginItem = PrimitiveUtils.getLeft(pivotRange);
        endItem = PrimitiveUtils.getRight(pivotRange);
      } else {
        // else determine the pivot range for the first time and
        // update the entry in the array
        TreeSet<Integer> items = partitionToItems.get(partitionId);
        beginItem = items.first();
        endItem = items.last();
        pivotRange = PrimitiveUtils.combine(beginItem, endItem);
        partitionToPivotRange[partitionId] = pivotRange;
      }

      // in case of a long transaction we only build a partition index
      // in this iteration
      // and call encoder afterwards for each distinct partition
      switch (FsmJob.getCommonConfig().getIndexingMethod()) {
      case NONE:
        // directly encode the transaction for this partition -- no
        // index is built
        // suffices to start encoding at "pos" (first pivot of this
        // partition)
        if (!FsmJob.getCommonConfig().isRemoveUnreachable()) {
          encode(transaction, partitionId, 0, transaction.length - 1, beginItem, endItem, false,
              true);
        } else {
          encode(transaction, partitionId, offset, transaction.length - 1, beginItem, endItem,
              false, true);
        }

        emit(partitionId, context);
        break;
      case MINMAX:
        if (isFirstOccurrence) {
          // new minimum offset, maximum offset has the same value
          partitionIndexMinMax[partitionId] = PrimitiveUtils.combine(offset, offset);
        } else {
          // new maximum offset
          long offsets = partitionIndexMinMax[partitionId];
          int minOffset = PrimitiveUtils.getLeft(offsets);
          partitionIndexMinMax[partitionId] = PrimitiveUtils.combine(minOffset, offset);
        }
        break;
      case FULL:
        int oldTail = -1;
        int tail = partitionIndexFullSize;
        partitionIndexFullSize++;
        if (!isFirstOccurrence) {
          oldTail = partitionIndexFullTail[partitionId];
          partitionIndexFullNext[oldTail] = tail;
        } else {
          // head is tail if this is the first occurrence
          partitionIndexFullHead[partitionId] = tail;
        }

        partitionIndexFullTail[partitionId] = tail;
        partitionIndexFullNext[tail] = -1;
        partitionIndexFullOffset[tail] = offset;

        break;

      }
    } // end for each transaction item

    // produce output when index is used
    switch (FsmJob.getCommonConfig().getIndexingMethod()) {
    case NONE:
      // done above
      break;
    case MINMAX:
      // for each distinct partition encode the transaction and emit
      // if non-empty
      // -- case where the partition index is built
      Iterator<Integer> partitionIt = partitionsOfTransaction.iterator();

      while (partitionIt.hasNext()) {
        int partitionId = partitionIt.next();

        pivotRange = partitionToPivotRange[partitionId];
        beginItem = PrimitiveUtils.getLeft(pivotRange);
        endItem = PrimitiveUtils.getRight(pivotRange);

        if (!EXPLOIT_ORDERING) {
          endItem = Integer.MAX_VALUE; // for debugging reasons
        }

        // retrieve the minimum and maximum offset of the pivots
        long pivotOffsets = partitionIndexMinMax[partitionId];
        int minOffset = PrimitiveUtils.getLeft(pivotOffsets);
        int maxOffset = PrimitiveUtils.getRight(pivotOffsets);

        // encode long transaction exploiting pivot offsets
        encode(transaction, partitionId, minOffset, maxOffset, beginItem, endItem, false, true);
        emit(partitionId, context);
      }
      break;
    case FULL:
      // for each distinct partition encode the transaction and emit
      // if non-empty
      // -- case where the partition index is built
      partitionIt = partitionsOfTransaction.iterator();

      while (partitionIt.hasNext()) {
        int partitionId = partitionIt.next();

        pivotRange = partitionToPivotRange[partitionId];
        beginItem = PrimitiveUtils.getLeft(pivotRange);
        endItem = PrimitiveUtils.getRight(pivotRange);

        if (!EXPLOIT_ORDERING) {
          endItem = Integer.MAX_VALUE; // for debugging reasons
        }

        // retrieve the [minimum, maximum] intervals
        int pos = partitionIndexFullHead[partitionId];

        // first occurrence of a pivot for this partition
        int minOffset = partitionIndexFullOffset[pos];
        int maxOffset = minOffset;

        pos = partitionIndexFullNext[pos];

        boolean append = false;

        // used in case splits are allowed to only finalize when all
        // splits across the transaction have been computed
        boolean finalize = false;

        while (pos >= 0) {
          int nextOffset = partitionIndexFullOffset[pos];

          if (nextOffset - neighborhoodSize <= maxOffset + neighborhoodSize) {
            maxOffset = nextOffset; // extend interval

          } else {
            // encode interval
            encode(transaction, partitionId, minOffset, maxOffset, beginItem, endItem, append,
                finalize);
            append = true;
            minOffset = nextOffset;
            maxOffset = nextOffset;
          }
          pos = partitionIndexFullNext[pos];

        }

        finalize = true;
        encode(transaction, partitionId, minOffset, maxOffset, beginItem, endItem, append,
            finalize);
        emit(partitionId, context);

      }
      break;
    }
  }

  /** Decodes a byte sequence (BytesWritable object) to an int[] array - for debugging purposes only
   * 
   * @param target the byte sequence to be decoded
   * @return the decoded byte sequence as int[] array
   * @throws IOException */
  public int[] decode(BytesWritable target) throws IOException {
    // decode the new sequence
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(target.getBytes(), 0,
        target.getLength()));

    int partitionId = WritableUtils.readVInt(in); // first byte is

    // initialize an integer array and copy/resize if needed
    int[] sequence = new int[100];
    int sequenceSize = 0;

    sequence[0] = partitionId;
    sequenceSize++;

    while (in.available() > 0) {

      // resize prevSequence if too short
      if (sequence.length == sequenceSize) {
        int[] temp = sequence;
        sequence = new int[2 * sequence.length];
        System.arraycopy(temp, 0, sequence, 0, temp.length);
      }

      // read item / gap
      sequence[sequenceSize] = WritableUtils.readVInt(in);
      sequenceSize++;
    }
    in.close();

    return Arrays.copyOf(sequence, sequenceSize);

  }

  /** Run encoding for (a part of) the transaction {@link BaseGapEncoder}. After encoding, call
   * {@link #emit(int, org.apache.hadoop.mapreduce.Mapper.Context)} to output the result.
   * 
   * @param transaction the transaction to be encoded
   * @param partitionId the id of the partition for which transaction is encoded
   * @param minOffset the minimum offset of a pivot -- default value (0)
   * @param maxOffset the maximum offset of a pivot -- default value (transaction.length - 1)
   * @param beginItem items in [beginItem,endItem] are pivot items
   * @param endItem items > endItem are irrelevant (i.e., will be treated as gaps)
   * @param append append to the current encoded sequence
   * @throws IOException
   * @throws InterruptedException */
  public void encode(int[] transaction, int partitionId, int minOffset, int maxOffset,
      int beginItem, int endItem, boolean append, boolean finalize) throws IOException {
    // encode the transaction and emit if encoding is non-empty
    if (FsmJob.getCommonConfig().isAllowSplits()) {

      // encode with splitting (output is a sequence of byte arrays)
      if (!append) {
        splitEncoder.clear();
        splitEncoder.setPartitionId(partitionId);
        splitEncoder.setSequenceLength(transaction.length);
      }
      splitEncoder.encode(transaction, minOffset, maxOffset, beginItem, endItem, append, finalize);
    } else {

      // encode without splitting (output is a byte array)
      if (!append) {
        simpleEncoder.clear();
        simpleEncoder.setPartitionId(partitionId);
      }
      simpleEncoder
          .encode(transaction, minOffset, maxOffset, beginItem, endItem, append, finalize);
    }
  }

  /** Emit the current encoded sequence (if non-empty).
   * 
   * @param partitionId the id of the partition for which transaction is encoded
   * @param context for emitting the result encoded transactions
   * @throws IOException
   * @throws InterruptedException */
  public void emit(int partitionId, Context context) throws IOException, InterruptedException {
    // compute encoded length of partitionId to check if encoded
    // transaction is non-empty
    int partitionIdVIntSize = WritableUtils.getVIntSize(partitionId);

    if (FsmJob.getCommonConfig().isAllowSplits()) {

      ArrayList<BytesWritable> targets = splitEncoder.targets();
      for (int i = 0; i < splitEncoder.getTargetsLength(); i++) {
        BytesWritable target = targets.get(i);
        if (target == null) {
          break;
        }
        if (target.getLength() > partitionIdVIntSize) {
          // i.e., there is at least one item
          context.write(target, supportWrapper);
        } else {
          break;
        }
      }
    } else {

      BytesWritable target = simpleEncoder.target();
      if (target.getLength() > partitionIdVIntSize) {
        context.write(target, supportWrapper);
        // int[] decodedSequence = decode(target); // FOR DEBUGGING
      }

    }
  }
}
