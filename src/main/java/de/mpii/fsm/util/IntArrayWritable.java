
package de.mpii.fsm.util;

//import gnu.trove.TIntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.mahout.math.list.IntArrayList;


/**
 * Writable wrapping an int[].
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
@SuppressWarnings("rawtypes")
public class IntArrayWritable implements WritableComparable {

    private int b = 0;

    private int e = 0;

    private int[] contents;

    public IntArrayWritable() {
        contents = new int[0];
    }

    public IntArrayWritable(int[] contents) {
        this.contents = contents;
        this.b = 0;
        this.e = contents.length;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVInt(d, e - b);
        for (int i = b; i < e; i++) {
            WritableUtils.writeVInt(d, contents[i]);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        contents = new int[WritableUtils.readVInt(di)];
        for (int i = 0; i < contents.length; i++) {
            contents[i] = WritableUtils.readVInt(di);
        }
        b = 0;
        e = contents.length;
    }

    @Override
    public int compareTo(Object o) {
        IntArrayWritable other = (IntArrayWritable) o;
        int length = e - b;
        int otherLength = other.e - other.b;
        int minLength = (length < otherLength ? length : otherLength);
        for (int i = 0; i < minLength; i++) {
            int tid = contents[b + i];
            int otherTId = other.contents[other.b + i];
            if (tid < otherTId) {
                return +1;
            } else if (tid > otherTId) {
                return -1;
            }
        }
        return (otherLength - length);
    }

    public int[] getContents() {
        int[] result = contents;
        if (b != 0 || e != contents.length) {
            result = new int[e - b];
            System.arraycopy(contents, b, result, 0, e - b);
        }
        return result;
    }

    public void setContents(int[] contents, int b, int e) {
        this.contents = contents;
        this.b = b;
        this.e = e;
    }

    public void setContents(int[] contents) {
        this.contents = contents;
        this.b = 0;
        this.e = contents.length;
    }

    public void setBegin(int b) {
        this.b = b;
    }

    public void setEnd(int e) {
        this.e = e;
    }

    public void setRange(int b, int e) {
        this.b = b;
        this.e = e;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        IntArrayWritable other = (IntArrayWritable) obj;

        if ((e - b) != (other.e - other.b)) {
            return false;
        }
        for (int i = 0, len = e - b; i < len; i++) {
            if (contents[b + i] != other.contents[other.b + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (contents == null) {
            return 0;
        }

        int result = 1;
        for (int i = b; i < e; i++) {
            result = 31 * result + contents[i];
        }

        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = b; i < e; i++) {
            sb.append(contents[i]);
            if (i != e - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static int lcp(int[] a, int[] b) {
        int lcp = 0;
        while (lcp < a.length && lcp < b.length && a[lcp] == b[lcp]) {
            lcp++;
        }
        return lcp;
    }

    public static int lcp(IntArrayList a, int[] b) {
        int lcp = 0;
        while (lcp < a.size() && lcp < b.length && a.get(lcp) == b[lcp]) {
            lcp++;
        }
        return lcp;
    }

    public static final class Comparator extends WritableComparator {

        public Comparator() {
            super(IntArrayWritable.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof IntArrayWritable && b instanceof IntArrayWritable) {
                return ((IntArrayWritable) a).compareTo((IntArrayWritable) b);
            }
            return super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstLength = readVInt(b1, s1);
                int p1 = s1 + WritableUtils.decodeVIntSize(b1[s1]);
                int secondLength = readVInt(b2, s2);
                int p2 = s2 + WritableUtils.decodeVIntSize(b2[s2]);
                int minLength = (firstLength < secondLength ? firstLength : secondLength);
                for (int i = 0; i < minLength; i++) {
                    int firstTId = readVInt(b1, p1);
                    p1 += WritableUtils.decodeVIntSize(b1[p1]);
                    int secondTId = readVInt(b2, p2);
                    p2 += WritableUtils.decodeVIntSize(b2[p2]);
                    if (firstTId < secondTId) {
                        return +1;
                    } else if (firstTId > secondTId) {
                        return -1;
                    }
                }
                return (secondLength - firstLength);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static final class ComparatorAscending extends WritableComparator {

        public ComparatorAscending() {
            super(IntArrayWritable.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof IntArrayWritable && b instanceof IntArrayWritable) {
                int[] aContents = ((IntArrayWritable) a).getContents();
                int[] bContents = ((IntArrayWritable) b).getContents();
                for (int i = 0, minlen = Math.min(aContents.length, bContents.length); i < minlen; i++) {
                    if (aContents[i] < bContents[i]) {
                        return -1;
                    } else if (aContents[i] > bContents[i]) {
                        return +1;
                    }
                }
                return (aContents.length - bContents.length);
            }
            return super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstLength = readVInt(b1, s1);
                int p1 = s1 + WritableUtils.decodeVIntSize(b1[s1]);
                int secondLength = readVInt(b2, s2);
                int p2 = s2 + WritableUtils.decodeVIntSize(b2[s2]);
                int minLength = (firstLength < secondLength ? firstLength : secondLength);
                for (int i = 0; i < minLength; i++) {
                    int firstTId = readVInt(b1, p1);
                    p1 += WritableUtils.decodeVIntSize(b1[p1]);
                    int secondTId = readVInt(b2, p2);
                    p2 += WritableUtils.decodeVIntSize(b2[p2]);
                    if (firstTId < secondTId) {
                        return -1;
                    } else if (firstTId > secondTId) {
                        return +1;
                    }
                }
                return (firstLength - secondLength);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    /**
     * Partitions based on first element of an <i>IntArrayWritable</i>.
     */
    public static final class PartitionerFirstOnly extends Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable key, Object value, int i) {
            int result = key.contents[key.b] % i;
            return (result < 0 ? -result : result);
        }
    }

    /**
     * Partitions based on complete contents of an <i>IntArrayWritable</i>.
     */
    public static final class PartitionerComplete extends Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable key, Object value, int i) {
            int result = 1;
            for (int j = key.b; j < key.e; j++) {
                result = result * 31 + key.contents[j];
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    public static final class DefaultComparator extends WritableComparator {

        public DefaultComparator() {
            super(IntArrayWritable.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof IntArrayWritable && b instanceof IntArrayWritable) {
                return ((IntArrayWritable) a).compareTo((IntArrayWritable) b);
            }
            return super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstLength = readVInt(b1, s1);
                int p1 = s1 + WritableUtils.decodeVIntSize(b1[s1]);
                int secondLength = readVInt(b2, s2);
                int p2 = s2 + WritableUtils.decodeVIntSize(b2[s2]);
                int minLength = (firstLength < secondLength ? firstLength : secondLength);
                for (int i = 0; i < minLength; i++) {
                    int firstTId = readVInt(b1, p1);
                    p1 += WritableUtils.decodeVIntSize(b1[p1]);
                    int secondTId = readVInt(b2, p2);
                    p2 += WritableUtils.decodeVIntSize(b2[p2]);
                    if (firstTId < secondTId) {
                        return +1;
                    } else if (firstTId > secondTId) {
                        return -1;
                    }
                }
                return (secondLength - firstLength);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static final class IntArrayPartitioner implements org.apache.hadoop.mapred.Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable k, Object v, int i) {
            int result = (31 * k.contents[k.b]) % i;
            return (result < 0 ? -result : result);
        }

        @Override
        public void configure(JobConf jc) {
        }
    }

    public static final class IntArrayWritablePartitionerAllButLast extends Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable key, Object value, int i) {
            int result = 1;
            int[] contents = key.getContents();
            for (int j = 0; j < contents.length - 1; j++) {
                result = result * 31 + contents[j];
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    public static final class IntArrayWritablePartitionerComplete extends Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable key, Object value, int i) {
            int result = 1;
            int[] contents = key.getContents();
            for (int j = 0; j < contents.length; j++) {
                result = result * 31 + contents[j];
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    public static final class IntArrayWritablePartitionerFirstTwo extends Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable key, Object value, int i) {
            int result = 1;
            int[] contents = key.getContents();
            for (int j = 0; j < contents.length && j < 2; j++) {
                result = result * 31 + contents[j];
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    public static final class IntArrayWritablePartitionerFirstOnly extends Partitioner<IntArrayWritable, Object> {

        @Override
        public int getPartition(IntArrayWritable key, Object value, int i) {
            int result = (31 * key.getContents()[0]) % i;
            return (result < 0 ? -result : result);
        }
    }
}
