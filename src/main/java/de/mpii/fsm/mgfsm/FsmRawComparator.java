package de.mpii.fsm.mgfsm;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A RawComparator for composite keys: the first part contains the partition id (encoded using 
 * variable-length encoding), the second part contains the actual sequence. Sequences
 * are sorted by id, then by length, then by binary content. This particular order is beneficial
 * when input transactions are buffered (so that long, difficult transactions appear last).
 * 
 * @author Iris Miliaraki
 * @author Rainer Gemulla
 */

public class FsmRawComparator extends WritableComparator {

  final int LENGTH_BYTES = 4;

  protected FsmRawComparator() {
    super(WritableComparable.class);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    // compare the identifiers
    int id1;
    int id2;
    try {
      id1 = WritableComparator.readVInt(b1, s1 + LENGTH_BYTES);
      id2 = WritableComparator.readVInt(b2, s2 + LENGTH_BYTES);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (id1 != id2) {
      return(new Integer(id1).compareTo(id2));
    }

    // compare length
    if (l1 != l2) {
      return(new Integer(l1).compareTo(l2));
    }

    // else compare content
    return WritableComparator.compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, 
        b2, s2 + LENGTH_BYTES, l2 - LENGTH_BYTES );
  }

}
