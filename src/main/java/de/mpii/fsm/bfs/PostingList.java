package de.mpii.fsm.bfs;

//import gnu.trove.TByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/** Utility methods for (de)compressing posting lists. 
 * 
 * @see BfsMiner#kPostingLists 
 */
final class PostingList {

  /** Appends compressed value v to the given posting list. Set v=0 for a separator, 
   * v=transactionId+1 for a transaction id, and v=position+1 for a position. */
  public static final void addCompressed(int v, ByteArrayList postingList) {
    assert v >= 0;
    do {
      byte b = (byte) (v & 127);
      v >>= 7;
      if (v == 0) {
        postingList.add(b);
        break;
      } else {
        b += 128;
        postingList.add(b);
      }
    } while (true);
  }

  /** Iterator-like decompression of posting lists. */
  static final class Decompressor {

    ByteArrayList postingList;

    int offset;

    public Decompressor() {
      this.postingList = null;
      this.offset = 0;

    }

    public Decompressor(ByteArrayList postingList) {
      this.postingList = postingList;
      this.offset = 0;
    }

    /** Is there another value in the posting? */
    public boolean hasNextValue() {
      return offset < postingList.size() && postingList.get(offset) != 0;
    }

    /** Returns the next transactionId/positopm in the posting. Throws an exception if the 
     * end of the posting has been reached (so be sure to use hasNextValue()). */
    int nextValue() {
      int result = 0;
      int shift = 0;
      do {
        byte b = postingList.get(offset);
        offset++;
        result += (b & 127) << shift;
        if (b < 0) {
          shift += 7;
        } else {
          break;
        }
      } while (true);
      return result - 1; // since we stored transactionId/positions incremented by 1
    }

    /** Moves to the next posting in the posting list and returns true if such a posting
     * exists. */
    boolean nextPosting() {
      do {
        offset++;
        if (offset >= postingList.size()) return false;
      } while (postingList.get(offset - 1) != 0); // previous byte is not a separator byte 
      return true;
    }
  }
}