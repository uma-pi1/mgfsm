package de.mpii.fsm.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Writable that represents a posting with positional information.
 * 
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class PostingWritable implements WritableComparable<PostingWritable> {

    private long did;

    private int[] offsets;

    public PostingWritable() {
        did = 0;
        offsets = new int[0];
    }

    public PostingWritable(long did, int[] offsets) {
        this.did = did;
        this.offsets = offsets;
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        did = WritableUtils.readVLong(di);
        offsets = new int[WritableUtils.readVInt(di)];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = (i == 0 ? WritableUtils.readVInt(di) : offsets[i - 1] + WritableUtils.readVInt(di));
        }
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVLong(d, did);
        WritableUtils.writeVInt(d, offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            if (i == 0) {
                WritableUtils.writeVInt(d, offsets[i]);
            } else {
                WritableUtils.writeVInt(d, offsets[i] - offsets[i - 1]);
            }
        }
    }

    public long getDId() {
        return did;
    }

    public int getTF() {
        return offsets.length;
    }

    public int[] getOffsets() {
        return offsets;
    }

    public void setDId(long did) {
        this.did = did;
    }

    public void setOffsets(int[] offsets) {
        this.offsets = offsets;
    }

    @Override
    public String toString() {
        return "( " + did + " | " + offsets.length + " | " + Arrays.toString(offsets) + " )";
    }

    @Override
    public int compareTo(PostingWritable t) {
        return (int) (did - t.did);
    }

}
