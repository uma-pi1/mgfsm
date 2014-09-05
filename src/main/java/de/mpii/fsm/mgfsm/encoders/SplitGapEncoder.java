package de.mpii.fsm.mgfsm.encoders;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

import de.mpii.fsm.util.IntArrayComparator;


/** Encodes an input sequence into a set of output subsequences. */
public class SplitGapEncoder extends BaseGapEncoder {

	// -- Subsequences ------------------------------------------------------------------------------
	/** Helper class that stores a subsequence and some index data structures on it. */
	private class Subsequence {
		public int[] sequence = new int[100];
		public int sequenceSize = 0;

		/** positions of pivots in sequence auxiliary structure for creating left & right indices */
		public int[] pivotsPos = new int[5];
		public int pivotsPosSize = 0;

		/** items left & right of pivots */
		public int[][] leftOfPivots = new int[5][gamma + 1];
		public int[][] rightOfPivots = new int[5][gamma + 1];

		/** Create indexes of the neighbor items at the left and right of each pivot of the subsequence.
		 * For each pivot item we index the neighbor items at a distance at most maxGap_ + 1 hops away
		 * from the pivot. */
		public void createPivotIndex() {
			// for each pivot
			for (int p = 0; p < pivotsPosSize; p++) {
				int pivotPos = pivotsPos[p];
				int next = -1;
				int neighborItems = 0;
				while (next != 0) { // first check left of pivot items & then right of pivot items
					int neighborPos = pivotPos + next;
					neighborItems = 0;
					int distanceFromPivot = 0;
					while (neighborPos >= 0 && neighborPos < sequenceSize && distanceFromPivot <= gamma) {
						int element = sequence[neighborPos];
						if (element < 0) {
							// found gap update distanceFromPivot
							distanceFromPivot += -element;
						} else {
							if (next == -1) {
								leftOfPivots[p][neighborItems++] = sequence[neighborPos];
							}
							if (next == 1) {
								rightOfPivots[p][neighborItems++] = sequence[neighborPos];
							}
							distanceFromPivot++;
						}
						neighborPos += next;
					}
					// sort leftOfPivots array ignoring empty positions
					if (neighborItems > 0 && next == -1) {				
						Arrays.sort(leftOfPivots[p], 0, neighborItems);
					}
					next = (next == -1) ? 1 : 0;
				}

				// sort rightOfPivots array ignoring empty positions
				if (neighborItems > 0) {
					Arrays.sort(rightOfPivots[p], 0, neighborItems);
				}
			}
		}

		/** update sequence & pivots, increase array lengths if needed */
		public void add(int v, boolean isPivot) {
			if (sequenceSize == sequence.length) {
				int[] oldSequence = sequence;
				sequence = new int[(sequenceSize * 3) / 2];
				System.arraycopy(oldSequence, 0, sequence, 0, oldSequence.length);
			}
			sequence[sequenceSize++] = v;

			if (isPivot) {
				if (pivotsPosSize == pivotsPos.length) {
					// increase pivotsPos length
					int[] oldPivotsPos = pivotsPos;
					pivotsPos = new int[pivotsPosSize * 2];
					System.arraycopy(oldPivotsPos, 0, pivotsPos, 0, oldPivotsPos.length);

					// increase also the size of left/right neighbor arrays
					int[][] oldLeft = leftOfPivots;
					leftOfPivots = new int[pivotsPosSize * 2][gamma + 1];
					System.arraycopy(oldLeft, 0, leftOfPivots, 0, oldLeft.length);
					int[][] oldRight = rightOfPivots;
					rightOfPivots = new int[pivotsPosSize * 2][gamma + 1];
					System.arraycopy(oldRight, 0, rightOfPivots, 0, oldRight.length);
				}

				// keep pivot position
				pivotsPos[pivotsPosSize++] = sequenceSize - 1;
			}
		}

		/** reset lengths */
		public void clear() {
			sequenceSize = 0;
			pivotsPosSize = 0;
		}
	}

	// -- variables ---------------------------------------------------------------------------------

	/** List of subsequences (both committed and uncommitted) */
	private List<Subsequence> subsequences;

	/** Flags for identifying subsequence merges */
	private int[] subsequencesFlags;

	/** Number of committed subsequences */
	private int committedSubsequences = 0;

	/** Keep whether neighbor items indices have been built for this subsequence */
	private boolean[] subsequencesIndices_;

	/** Filled during finalize step with all subsequences */
	private final ArrayList<BytesWritable> targets;
	private int targetsLength;

	/** Partition identifier to be added at the beginning of each subsequence */
	private int partitionId;

	/** Length of sequence before splitting for initializing the size of the output buffers */
	private int sequenceLength;

	/** Allow dropping of redundant gaps */
	private boolean dropGaps = false;

	// -- construction ------------------------------------------------------------------------------

	/** Creates a new SplitGapEncoder.
	 * 
	 * @param gamma maximum gap allowed within a subsequence
	 * @param stopWords set of stop words
	 * @param simpleTarget BytesWritable that is used to store the output. Ownership of target is
	 *          transferred to this encoder, i.e., the variable should not be modified outside. */
	public SplitGapEncoder(int gamma, int lambda, ArrayList<BytesWritable> targets) {
		super(gamma, lambda);
		this.targets = targets;
		this.targetsLength = targets.size();
		subsequences = new ArrayList<Subsequence>();
		subsequences.add(new Subsequence());
		committedSubsequences = 0;
	}
	
	
	public SplitGapEncoder(int gamma, int lambda, ArrayList<BytesWritable> targets, boolean compressGaps, boolean removeUnreachable) {
		super(gamma, lambda, compressGaps, removeUnreachable);
		this.targets = targets;
		this.targetsLength = targets.size();
		subsequences = new ArrayList<Subsequence>();
		subsequences.add(new Subsequence());
		committedSubsequences = 0;
	}

	// -- I/O methods -------------------------------------------------------------------------------

	@Override
	public void writePartitionId() throws IOException {
		// do nothing
	}

	@Override
	public void writeGap(int gap) throws IOException {
		assert gap > 0;
		// write all gaps together
		subsequences.get(committedSubsequences).add(-gap, false);
	}

	@Override
	public void writeItem(int item, boolean isPivot) throws IOException {
		assert item > 0;
		subsequences.get(committedSubsequences).add(item, isPivot);
	}

	@Override
	public void commit() throws IOException {
		committedSubsequences++;
		if (committedSubsequences >= subsequences.size()) {
			subsequences.add(new Subsequence());
		} else {
			subsequences.get(committedSubsequences).clear();
		}

		// subsequences_.get(committedSubsequences_ - 1).dropRedundantGaps();
	}

	public void optimizeAll() {
		// Flags for subsequences
		// 0: subsequence not yet checked
		// -1: subsequence can be omitted e.g., same subsequence exists
		// >0: subsequence merged with subsequence at position = flag - 1
		subsequencesFlags = new int[committedSubsequences]; // TODO: avoid new

		// initially no index is built
		subsequencesIndices_ = new boolean[committedSubsequences]; // TODO: avoid new
		for (int subseq1 = 0; subseq1 < committedSubsequences; subseq1++) {
			if (subsequencesFlags[subseq1] == 0) {
				subsequencesFlags[subseq1] = subseq1 + 1;
			}

			if (subsequencesFlags[subseq1] == -1) {
				continue;
			}

			for (int subseq2 = subseq1 + 1; subseq2 < committedSubsequences; subseq2++) {
				if (subsequencesFlags[subseq2] == -1) {
					continue;
				}
				boolean canBeSeparated = true;
				Subsequence s1 = subsequences.get(subseq1), s2 = subsequences.get(subseq2);

				// first check equivalence
				if (IntArrayComparator
						.compare(s1.sequence, s2.sequence, s1.sequenceSize, s2.sequenceSize) == 0) {
					// subsequence can be omitted set flag to -1
					subsequencesFlags[subseq2] = -1;
					continue;
				}

				for (int pivotPos1 = 0; pivotPos1 < s1.pivotsPosSize; pivotPos1++) {
					if (!canBeSeparated) {
						break;
					}

					for (int pivotPos2 = 0; pivotPos2 < s2.pivotsPosSize; pivotPos2++) {
						int pivot1 = s1.sequence[s1.pivotsPos[pivotPos1]];
						int pivot2 = s2.sequence[s2.pivotsPos[pivotPos2]];

						if (pivot1 == pivot2) { // if subsequences share a pivot
							// build neighbor indices if not built yet
							if (!subsequencesIndices_[subseq1]) {
								subsequences.get(subseq1).createPivotIndex();
								subsequencesIndices_[subseq1] = true;
							}

							if (!subsequencesIndices_[subseq2]) {
								subsequences.get(subseq2).createPivotIndex();
								subsequencesIndices_[subseq2] = true;
							}

							boolean checkedLeft = false, checkedRight = false;
							while ((!checkedLeft || !checkedRight) && canBeSeparated) {
								int[] neighborItemsS1, neighborItemsS2;

								if (!checkedLeft) {
									neighborItemsS1 = s1.leftOfPivots[pivotPos1];
									neighborItemsS2 = s2.leftOfPivots[pivotPos2];
								} else {
									neighborItemsS1 = s1.rightOfPivots[pivotPos1];
									neighborItemsS2 = s2.rightOfPivots[pivotPos2];
									checkedRight = true;
								}
						
								int s1Pos = 0;
								int s2Pos = 0;

								// merge join neighbor arrays to check whether there is an overlap
								while (s1Pos < neighborItemsS1.length && s2Pos < neighborItemsS2.length) {
									int neighborItem1 = neighborItemsS1[s1Pos];
									int neighborItem2 = neighborItemsS2[s2Pos];

									if (neighborItem1 == neighborItem2) {
										if (neighborItem1 != 0) {
											canBeSeparated = false;
											break;
										} else {
											s1Pos++;
											s2Pos++;
											continue;
										}
									}

									if (neighborItem1 > neighborItem2) {
										s2Pos++;
									} else { // if (neighborItem1 < neighborItem2) {
										s1Pos++;
									}

								}

								checkedLeft = true;
							}
						}
					}
				}

				if (!canBeSeparated) {
					int flag1 = subsequencesFlags[subseq1];
					int flag2 = subsequencesFlags[subseq2];

					if (flag2 != 0) {
						// subsequencesFlags_[subseq1] = subsequencesFlags_[subseq2];
						subsequencesFlags[subseq1] = flag2;
						for (int i = 0; i < committedSubsequences; i++) {
							if (subsequencesFlags[i] == flag1) {
								subsequencesFlags[i] = flag2;
							}
						}
					} else if (flag2 == 0) {
						subsequencesFlags[subseq2] = subsequencesFlags[subseq1];
					}
				}
			}
		}
	}

	@Override
	public void rollback() throws IOException {
		subsequences.get(committedSubsequences).clear();
	}

	@Override
	public void finalize() throws IOException {
		// do optimization at this step
		optimizeAll();

		// output
		writeToTargets();
	}

	/** Write subsequences to the target buffers.
	 * 
	 * @throws IOException */
	public void writeToTargets() throws IOException {
		HashMap<Integer, Integer> flagToTargetsPos = new HashMap<Integer, Integer>();

		for (int i = 0; i < subsequencesFlags.length; i++) {
			int flag = subsequencesFlags[i];
			switch (flag) {
			case 0:
				// should not enter here
				break;
			case -1:
				continue;
			default:
				Integer targetPos = flagToTargetsPos.get(flag - 1);
				if (targetPos == null) {
					flagToTargetsPos.put(flag - 1, flagToTargetsPos.size());
					writeSequence(i, flagToTargetsPos.size() - 1);
				} else {
					writeSequence(i, targetPos);
				}
			}
		}
	}

	public void writeSequence(int subseqPos, int targetsPos) throws IOException {
		Subsequence subseq = subsequences.get(subseqPos);
		boolean isEmpty = false;

		while (targetsPos >= targetsLength) {
			// initialize target and make sure it is large enough
			BytesWritable temp = new BytesWritable();
			temp.setSize(0);
			temp.setCapacity(sequenceLength * 9);
			if (targetsPos >= targets.size()) {
				targets.add(temp);
			} else {
				targets.set(targetsPos, temp);
			}

			isEmpty = true;
			targetsLength++;
		}
		final BytesWritable target = targets.get(targetsPos);

		// create an output stream that writes to the target
		OutputStream sout = new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				int length = target.getLength();
				target.getBytes()[length] = (byte) b;
				target.setSize(length + 1);
			}
		};
		DataOutputStream out = new DataOutputStream(sout);

		// first write the partition identifier or a gap as separator
		if (isEmpty) {
			WritableUtils.writeVInt(out, partitionId);
		} else {
			if (compressGaps) {
				WritableUtils.writeVInt(out, -(gamma + 1));
			} else {
				for (int i = 0; i < gamma + 1; i++) {
					WritableUtils.writeVInt(out, -1);
				}
			}
		}

		// then write items & gaps
		for (int it = 0; it < subseq.sequenceSize; it++) {
			WritableUtils.writeVInt(out, subseq.sequence[it]);
		}

		out.close();
		sout.close();

	}

	@Override
	public void clear() throws IOException {
		committedSubsequences = 0;
		subsequences.get(committedSubsequences).clear();
		targetsLength = 0;
		// targets.clear();
	}

	// -- getters/setters
	// ---------------------------------------------------------------------------
	/** Returns the BytesWritable array that stores the encoded subsequences. The return value is owned
	 * by this encoder, do not modify! */
	public ArrayList<BytesWritable> targets() {
		return targets;
	}

	public int getTargetsLength() {
		return targetsLength;
	}

	/** Sets the partition identifier, should be called after initializing & clear methods */
	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	/** Sets the maximum sequence length, should be called after initializing & clear methods */
	public void setSequenceLength(int maxSequenceLength) {
		sequenceLength = maxSequenceLength;
	}

	/** Enables or disables the reduction of a sequence by dropping redundant gaps
	 * 
	 * @return true if it is enabled and false if it is disabled */
	public boolean isDropGaps() {
		return dropGaps;
	}

	/** Sets whether dropping of gaps is allowed for reducing a subsequence
	 * 
	 * @param dropGaps true if it is enabled and false if it is disabled */
	public void setDropGaps(boolean dropGaps) {
		this.dropGaps = dropGaps;
	}

	// -- main method -------------------------------------------------------------------------------

	/** Some examples */
	public static void main(String args[]) throws IOException {

		// check containment
		testWriteSequence(new int[] { 1, 310, 2, 1000, 1000, 18, 1000, 89, 127, 1, 310, 2 }, 310, 310,
				1);
		testWriteSequence(new int[] { 1, 3, 6, 100, 3, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 100, 3, 6, 2, 100, 100, 1, 3, 6, 100, 2 }, 6, 8, 1);

		// testWriteSequence(new int[] { 1, 2, 3, 6, 100, 1, 2, 6 }, 6, 8, 0);

		// no common pivot
		testWriteSequence(new int[] { 1, 3, 6, 100, 1, 4, 3, 7 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 1, 3, 6, 100, 1, 3, 4, 7 }, 6, 8, 0);
		// only pivot common
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 4, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 100, 2, 4, 6 }, 6, 8, 1);
		// common pivot and other
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 3, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 3, 2, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 100, 2, 3, 6 }, 6, 8, 1);
		// more than 1 common pivots and no other
		testWriteSequence(new int[] { 1, 7, 6, 100, 2, 7, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 7, 1, 6, 100, 2, 7, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 7, 3, 1, 6, 100, 4, 1, 6 }, 6, 7, 0);
	}

	public static void testWriteSequence(int[] sentence, int beginId, int endId, int maxGap)
			throws IOException {

		int partitionId = 1000;

		// stopWordsSet.add(6);
		SplitGapEncoder encoder = new SplitGapEncoder(maxGap, 5, new ArrayList<BytesWritable>());
		encoder.setPartitionId(partitionId);
		encoder.setSequenceLength(sentence.length);

		// print sentence
		System.out.println();
		System.out.println("Sequence: " + Arrays.toString(sentence) + " / id range: " + beginId + "-"
				+ endId + " / max gap: " + maxGap);

		// print result
		encoder.encode(sentence, 0, sentence.length - 1, beginId, endId, false, true);
		ArrayList<BytesWritable> targets = encoder.targets();

		for (int i = 0; i < targets.size(); i++) {
			if (targets.get(i) == null || targets.get(i).getLength() == 0) {
				continue;
			}

			ByteArrayInputStream bin = new ByteArrayInputStream(targets.get(i).getBytes(), 0, targets
					.get(i).getLength());
			DataInputStream in = new DataInputStream(bin);

			System.out.print("Encoded subsequence has " + in.available() + " byte(s): ");
			while (in.available() > 0) {
				long v = WritableUtils.readVInt(in);
				System.out.print(v + " ");
			}
			System.out.println();
		}

	}
}
