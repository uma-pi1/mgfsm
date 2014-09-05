package de.mpii.fsm.mgfsm.encoders;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

/** Encodes an input sequence into a single output sequence. */
public class SimpleGapEncoder extends BaseGapEncoder {

	// -- variables
	// ---------------------------------------------------------------------------------

	/**
	 * Holds both committed output bytes (up to target_.getLength()) and
	 * uncommitted output bytes (what follows up to uncommittedLength_)
	 */
	private final BytesWritable target;

	/**
	 * Appends uncommitted output to target. When writing to this stream,
	 * uncommitedLength will be updated.
	 */
	private final DataOutputStream targetOut;

	/** Number of bytes currently uncommitted */
	private int uncommittedLength;

	/** Partition identifier to be written before the actual sequence */
	private int partitionId;

	// -- construction
	// ------------------------------------------------------------------------------
	/**
	 * Creates a new SimpleGapEncoder.
	 * 
	 * @param maxGap
	 *            maximum gap allowed within a subsequence
	 * @param stopWords
	 *            set of stop words
	 * @param targt
	 *            BytesWritable that is used to store the output. Ownership of
	 *            target is transferred to this encoder, i.e., the variable
	 *            should not be modified outside.
	 */
	// -- construction ------------------------------------------------------------------------------
	/** Creates a new SimpleGapEncoder.
	 * 
	 * @param maxGap maximum gap allowed within a subsequence
	 * @param stopWords set of stop words
	 * @param targt BytesWritable that is used to store the output. Ownership of target is
	 *          transferred to this encoder, i.e., the variable should not be modified outside. */
	public SimpleGapEncoder(int maxGap, int maxSequenceLength, BytesWritable targt) {
		super(maxGap, maxSequenceLength);
		this.target = targt;

		OutputStream sout = new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				if (target.getCapacity() < uncommittedLength + 1) {
					assert target.getCapacity() == uncommittedLength;
					int l = target.getLength();
					target.setSize(uncommittedLength);
					target.setCapacity(uncommittedLength + 1);
					target.setSize(l);
				}
				target.getBytes()[uncommittedLength] = (byte) b;
				uncommittedLength++;
			}
		};
		targetOut = new DataOutputStream(sout);
	}
	
	
	public SimpleGapEncoder(int maxGap, int maxSequenceLength, BytesWritable targt, boolean compressGaps, boolean removeUnreachable) {
		super(maxGap, maxSequenceLength, compressGaps, removeUnreachable);
		this.target = targt;

		OutputStream sout = new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				if (target.getCapacity() < uncommittedLength + 1) {
					assert target.getCapacity() == uncommittedLength;
					int l = target.getLength();
					target.setSize(uncommittedLength);
					target.setCapacity(uncommittedLength + 1);
					target.setSize(l);
				}
				target.getBytes()[uncommittedLength] = (byte) b;
				uncommittedLength++;
			}
		};
		targetOut = new DataOutputStream(sout);
	}


	// -- I/O methods
	// -------------------------------------------------------------------------------
	@Override
	public void writePartitionId() throws IOException {
		assert partitionId > 0;
		WritableUtils.writeVInt(targetOut, partitionId);
		target.setSize(uncommittedLength);
	}

	@Override
	public void writeGap(int gap) throws IOException {
		assert gap > 0;

		// compress gaps using a single integer
		WritableUtils.writeVInt(targetOut, -gap);
	}

	@Override
	public void writeItem(int item, boolean isPivot) throws IOException {
		assert item > 0;
		WritableUtils.writeVInt(targetOut, item);
	}

	@Override
	public void commit() throws IOException {
		target.setSize(uncommittedLength);
		if (compressGaps) {
			writeGap(gamma + 1);
		} else {
			writeGapUncompressed(gamma + 1);
		}
	}

	@Override
	public void rollback() throws IOException {
		uncommittedLength = target.getLength();
		if (uncommittedLength != 0) {
			if (compressGaps) {
				writeGap(gamma + 1);
			} else {
				writeGapUncompressed(gamma + 1);
			}
		}
	}

	@Override
	public void finalize() throws IOException {
		// nothing to be done
	}

	@Override
	public void clear() throws IOException {
		target.setSize(0);
		uncommittedLength = 0;
	}

	// -- getters/setters
	// ---------------------------------------------------------------------------
	/**
	 * Returns the BytesWritable that stores the encoded sequence. The return
	 * value is owned by this encoder, do not modify!
	 */
	public BytesWritable target() {
		return target;
	}

	/** Sets the partition identifier, should be called after clear() */
	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	// -- main method
	// -------------------------------------------------------------------------------
	/** Some examples */
	public static void main(String args[]) throws IOException {

		// no common pivot
		testWriteSequence(new int[] { 1, 3, 6, 100, 1, 3, 4, 7 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 100, 1, 3, 4, 7 }, 6, 8, 1);

		// only pivot common
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 4, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 100, 2, 4, 6 }, 6, 8, 1);
		// common pivot and other
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 3, 6 }, 6, 8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 100, 2, 3, 6 }, 6, 8, 1);

		// more than 1 common pivots and no other
		testWriteSequence(new int[] { 1, 7, 6, 100, 2, 7, 6 }, 6, 8, 0);

		// other cases
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 3, 6, 100, 4, 5, 6 }, 6,
				8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 4, 5, 6, 100, 2, 3, 6 }, 6,
				8, 0);
		testWriteSequence(new int[] { 4, 5, 6, 100, 1, 3, 6, 100, 2, 3, 6 }, 6,
				8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 4, 6, 100, 2, 4, 6 }, 6,
				8, 0);
		testWriteSequence(new int[] { 1, 3, 6, 100, 2, 4, 6, 100, 2, 4, 7 }, 6,
				8, 0);
		testWriteSequence(new int[] { 100, 100, 1, 3, 6, 100, 2, 3, 6, 100, 4,
				3, 6, 100, 100 }, 6, 8, 0);
	}

	private static void testWriteSequence(int[] sentence, int beginId,
			int endId, int maxGap) throws IOException {
		int partitionId = 1;
		SimpleGapEncoder encoder = new SimpleGapEncoder(maxGap, 3,
				new BytesWritable());
		encoder.setPartitionId(partitionId);

		// print sentence
		System.out.println();
		System.out.println("Sequence: " + Arrays.toString(sentence)
				+ " / id range: " + beginId + "-" + endId + " / max gap: "
				+ maxGap);

		encoder.encode(sentence, 0, sentence.length - 1, beginId, endId, false,
				true);
		ByteArrayInputStream bin = new ByteArrayInputStream(encoder.target()
				.getBytes(), 0, encoder.target().getLength());
		DataInputStream in = new DataInputStream(bin);
		System.out.print("Encoded sequence has " + in.available()
				+ " byte(s): ");
		while (in.available() > 0) {
			long v = WritableUtils.readVInt(in);
			System.out.print(v + " ");
		}
		System.out.println();
	}
}
