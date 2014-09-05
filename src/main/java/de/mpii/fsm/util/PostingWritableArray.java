package de.mpii.fsm.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


/**
 * Writable that represents an array of postings.
 * 
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class PostingWritableArray implements Writable {

    private String term = "";

    private PostingWritable[] postings;

    public PostingWritableArray() {
        postings = new PostingWritable[0];
    }

    public PostingWritableArray(PostingWritable[] postings) {
        this.postings = postings;
    }

    public PostingWritableArray(String term, PostingWritable[] postings) {
        this.term = term;
        this.postings = postings;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(term + " : ");
        for (PostingWritable posting : postings) {
            sb.append(posting + " ");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeCompressedString(d, term);
        WritableUtils.writeVInt(d, postings.length);
        for (int i = 0; i < postings.length; i++) {
            PostingWritable posting = postings[i];
            if (i == 0) {
                WritableUtils.writeVLong(d, posting.getDId());
            } else {
                WritableUtils.writeVLong(d, posting.getDId() - postings[i - 1].getDId());
            }
            int[] offsets = posting.getOffsets();
            WritableUtils.writeVInt(d, offsets.length);
            for (int j = 0; j < offsets.length; j++) {
                if (j == 0) {
                    WritableUtils.writeVInt(d, offsets[j]);
                } else {
                    WritableUtils.writeVInt(d, offsets[j] - offsets[j - 1]);
                }
            }
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        term = WritableUtils.readCompressedString(di);
        postings = new PostingWritable[WritableUtils.readVInt(di)];
        for (int i = 0; i < postings.length; i++) {
            long did = (i == 0 ? 0 : postings[i - 1].getDId());
            did += WritableUtils.readVLong(di);
            int[] offsets = new int[WritableUtils.readVInt(di)];
            for (int j = 0; j < offsets.length; j++) {
                offsets[j] = (j == 0 ? 0 : offsets[j - 1]);
                offsets[j] += WritableUtils.readVInt(di);
            }
            postings[i] = new PostingWritable(did, offsets);
        }
    }

    public void setTerm(String term) {
        this.term = term;
    }
    
    public void set(PostingWritable[] postings) {
        this.postings = postings;
    }
    
    public String getTerm() {
        return term;
    }

    public PostingWritable[] get() {
        return postings;
    }

}
