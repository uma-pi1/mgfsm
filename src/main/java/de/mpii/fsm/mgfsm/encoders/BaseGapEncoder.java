package de.mpii.fsm.mgfsm.encoders;

import java.io.IOException;

public abstract class BaseGapEncoder {

	// -- variables
	// ---------------------------------------------------------------------------------
	/** the maximum gap allowed within a subsequence */
	protected int gamma;

	/** the maximum length of frequent subsequences */
	protected int lambda;

	/**
	 * left reachability is measured as the minimum number of hops to reach a
	 * pivot at the left from current item
	 */
	protected int[] leftHops;

	/**
	 * right reachability is measured as the minimum number of hops to reach a
	 * pivot at the right from current item
	 */
	protected int[] rightHops;

	/** auxiliary variables for computing distances */
	protected int lastPdist = -1; // distance of last reachable item from the
	// pivot
	protected int lastHops = -1; // number of hops to reach that item
	protected int prevPdist = -1; // distance from pivot of last reachable item
	// with smaller
	// distance

	/** minimum allowed length */
	protected final int MINIMUM_LENGTH = 2;

	/** whether unreachable items are removed */
	protected boolean removeUnreachable = true;

	/**
	 * whether gaps will be compressed (multiple consecutive gaps grouped
	 * together, leading and trailing gaps removed)
	 */
	protected boolean compressGaps = true;

	// -- construction
	// ------------------------------------------------------------------------------
	/**
	 * Creates a new SimpleGapEncoder.
	 * 
	 * @param maxGap
	 *            maximum gap allowed within a subsequence
	 * @param stopWords
	 *            set of stop words
	 */
	public BaseGapEncoder(int gamma, int lambda, boolean compressGaps,
			boolean removeUnreachable) {
		this.gamma = gamma;
		this.lambda = lambda;
		this.compressGaps = compressGaps;
		this.removeUnreachable = removeUnreachable;
		if (this.removeUnreachable && !this.compressGaps) {
			throw new IllegalStateException();
		}

		// if unreachable items will be removed initialize structures for
		// computing
		// reachability metric
		if (this.removeUnreachable) {
			leftHops = new int[100];
			rightHops = new int[100];
		}
	}

	public BaseGapEncoder(int gamma, int lambda) {
		this(gamma, lambda, true, true);
	}

	// -- encoding
	// ----------------------------------------------------------------------------------
	/**
	 * Breaks the input transaction into multiple subsequences separated by a
	 * gap larger than gamma. All items larger than endItem are treated as gaps
	 * (of length 1). Multiple consecutive gaps can be grouped together (i.e., 3
	 * gaps of length 1 = 1 gap of length 3). Leading and trailing gaps of a
	 * subsequence are not encoded. Every subsequence is guaranteed to contain
	 * at least 2 items, at least one of which is in the pivot set (i.e., in
	 * [beginItem, endItem]). In case of long transactions, minOffset and
	 * maxOffset are taken into account to avoid iterating through the whole
	 * transaction.
	 * 
	 * A call to this method appends to the current output; use {@code
	 * #finalize()} after processing an entire transaction.
	 * 
	 * @param transaction
	 *            long input transaction s
	 * @param minPivotOffset
	 *            the smallest offset of a pivot item -- default value (0)
	 * @param maxPivotOffset
	 *            the largest offset of a pivot item -- default value
	 *            (transaction.length - 1)
	 * @param beginItem
	 *            items in [beginItem,endItem] are pivot items
	 * @param endItem
	 *            items > endItem are irrelevant (i.e., treated as gaps)
	 * 
	 * @throws IOException
	 */
	public void encode(int[] transaction, int minPivotOffset,
			int maxPivotOffset, int beginItem, int endItem, boolean append,
			boolean finalize) throws IOException {

		// whether previous and last should be reset because we now begin
		// computation of left/right hops
		boolean reset = true;

		// a reachable item can be at most lambda * (gamma + 1) hops away from a
		// pivot
		int maxHops = (lambda - 1) * (gamma + 1);

		int len = transaction.length;

		// iterate transaction between [leftMostReachablePos,
		// rightMostReachablePos]
		int leftMostReachablePos = (minPivotOffset - maxHops >= 0) ? minPivotOffset
				- maxHops
				: 0;
		int rightMostReachablePos = (maxPivotOffset + maxHops < len) ? maxPivotOffset
				+ maxHops
				: len - 1;

		if (removeUnreachable) {

			// if the arrays for keeping the left/right hops are not large
			// enough,
			// increase their size
			if (leftHops.length <= len) {
				increaseHopsLength(len);
			}

			// before scanning the input transaction from left to right, scan
			// from
			// right to left to compute right reachability
			int pdist = Integer.MAX_VALUE; // distance from pivot (to the right,
			// != hops))
			for (int pos = rightMostReachablePos; pos >= leftMostReachablePos
					&& pos >= 0; pos--) {
				int item = transaction[pos];
				boolean isRelevant = item <= endItem && item > 0;
				boolean isPivot = isRelevant && item >= beginItem;

				updateHops(pos, pdist, isPivot, isRelevant, reset, rightHops);
				reset = false;

				// update pivot distance (for next item)
				if (item < 0) {
					pdist += -item;
				} else if (isPivot) {
					pdist = 1;
				} else {
					pdist++;
				}

				// System.out.println("Right hops " + rightHops[pos]);
			}

			// reset again since computation of right hops finished
			reset = true;
		}

		// auxiliary variables
		int gap = 0; // current size of gap in input
		int uncommittedItems = 0; // how many items are uncommitted?
		boolean uncommittedPivot = false; // is there an uncommitted pivot?

		// first write identifier
		if (!append)
			writePartitionId();

		// main loop
		// we write out items until the end of a subsequence (i.e., a gap >
		// gamma)
		// if the output conditions hold, we then commit; else we rollback
		int pdist = Integer.MAX_VALUE;
		for (int position = leftMostReachablePos; position <= rightMostReachablePos; position++) {
			// get the next item
			int item = transaction[position];
			boolean isRelevant = item <= endItem && item > 0;
			boolean isPivot = isRelevant && item >= beginItem;

			boolean isReachable = true;
			if (removeUnreachable) {
				// compute left hops forward scanning sequence
				updateHops(position, pdist, isPivot, isRelevant, reset,
						leftHops);
				reset = false;

				// update pivot distance (for next item)
				if (item < 0) {
					pdist += -item;
				} else if (isPivot) {
					pdist = 1;
				} else {
					pdist++;
				}

				// System.out.println("Left hops " + leftHops[position]);
				isReachable = Math.min(leftHops[position], rightHops[position]) < lambda;
			}

			// process item
			if (!isRelevant) { // i.e., irrelevant item
				// skip irrelevant items at the beginning of a subsequence
				// unless gap should not be compressed
				if ((uncommittedItems == 0 && compressGaps) || !isReachable) {
					continue;
				}

				// otherwise the gap increased by 1 or the
				// appropriate gap length in case of input gap
				if (item < 0) {
					gap += -item;
				} else {
					gap++;
				}

				// if the gap is larger than maxGap, we start a new subsequence
				if (gap > gamma && removeUnreachable) { // || isSeparationPoint)
					// {
					if (uncommittedPivot && uncommittedItems > 1) {
						commit();
					} else {
						rollback();
					}

					// start a new subsequence
					gap = 0;
					pdist = Integer.MAX_VALUE;
					uncommittedPivot = false;
					uncommittedItems = 0;
				}
			} else { // relevant item
				// if item is unreachable drop it
				if (!isReachable) {
					continue;
				}

				// if there are any gaps, we need to append them to the buffer
				if (gap > 0) {
					// compress and write all gaps together
					if (compressGaps) {
						writeGap(gap);
					} else {
						// no compression is used, each gap is written
						// separately
						writeGapUncompressed(gap);
					}
					gap = 0;
					// gapLength = 0;
				}

				// now append the item and a flag whether it is pivot to the
				// buffer
				uncommittedPivot |= isPivot;
				writeItem(item, isPivot);
				uncommittedItems++;
			}
		}

		// rollback if necessary
		if (!compressGaps) {
			if (gap > 0) {
				writeGapUncompressed(gap);
				commit();
			} else if (uncommittedItems > 0) {
				commit();
			}
		} else { // we compress gaps
			if (!removeUnreachable) {
				if (uncommittedItems > 0) {
					commit();
				} else {
					rollback();
				}
			} else { // we remove unreachable items
				if (uncommittedPivot && uncommittedItems >= MINIMUM_LENGTH) {
					commit();
				} else {
					rollback();
				}
			}
		}
		if (finalize)
			finalize();
	}

	// -- methods for computing reachability metric
	// -------------------------------------------------
	/**
	 * Compute the minimum number of hops from position pos to the closest pivot
	 * to the right.
	 * 
	 * @param pos
	 *            the position of the current item in the sequence
	 * @param pdist
	 *            the distance of the current item from the last pivot (counting
	 *            gaps, too); ignored if isPivot==true
	 * @param isPivot
	 *            whether or not the current item is a pivot
	 * @param isRelevant
	 *            whether or not the current item is a relevant item
	 * @param reset
	 *            whether previous_ and last_ should be reset
	 * @param hops
	 *            the hops array to update
	 */
	private void updateHops(int pos, int pdist, boolean isPivot,
			boolean isRelevant, boolean reset, int[] hops) {
		if (reset) {
			prevPdist = -1; // last reachable item at lower distance
			lastPdist = -1; // last reachable item
		}
		if (isPivot) { // pivot item
			hops[pos] = 0;
			lastPdist = 0;
			prevPdist = 0;
			lastHops = 1;
		} else if (lastPdist == -1 || pdist - lastPdist > gamma + 1) { // not
			// reachable
			// from
			// last
			hops[pos] = Integer.MAX_VALUE;
		} else if (!isRelevant) { // update distances for irrelevant items also
			hops[pos] = lastHops;
			if (pdist - prevPdist > gamma + 1) {
				hops[pos]++;
			}
		} else {
			if (pdist - prevPdist > gamma + 1) {
				prevPdist = lastPdist;
				lastHops++;
			}
			lastPdist = pdist;
			hops[pos] = lastHops;
		}
	}

	/** Increase length of arrays for storing left/right hops */
	private void increaseHopsLength(int length) {
		assert length > leftHops.length;
		int[] oldLeftHops = leftHops;
		int[] oldRightHops = rightHops;
		leftHops = new int[(length * 3) / 2];
		rightHops = new int[(length * 4) / 2];
		System.arraycopy(oldLeftHops, 0, leftHops, 0, oldLeftHops.length);
		System.arraycopy(oldRightHops, 0, rightHops, 0, oldRightHops.length);
	}

	public int gamma() {
		return gamma;
	}

	public int lambda() {
		return lambda;
	}

	// -- abstract I/O methods
	// ----------------------------------------------------------------------
	/** Append an identifier to the current subsequence (uncommitted write). */
	public abstract void writePartitionId() throws IOException;

	/**
	 * Append a gap (compressed) to the current subsequence (uncommitted write).
	 */
	public abstract void writeGap(int gap) throws IOException;

	/**
	 * Append all gaps (uncompressed) to the current subsequence (uncommitted
	 * write).
	 */
	public void writeGapUncompressed(int gap) throws IOException {
		assert gap > 0;
		// write each gap separately (gap integers are used)
		for (int i = 0; i < gap; i++) {
			writeGap(1);
		}
	}

	/** Append an item to the current subsequence (uncommitted write). */
	public abstract void writeItem(int item, boolean isPivot)
			throws IOException;

	/** Commit the current subsequence and start a new one. */
	public abstract void commit() throws IOException;

	/** Rollback the current subsequence. */
	public abstract void rollback() throws IOException;

	/**
	 * Finalize after all subsequences have been written. Guaranteed to be
	 * called directly after commit() or rollback().
	 */
	public abstract void finalize() throws IOException;

	/** Start from scratch. */
	public abstract void clear() throws IOException;
}
