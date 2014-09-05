package de.mpii.fsm.mgfsm.maximal;

/**
 * @author kbeedkar(kbeedkar@mpi-inf.mpg.de)
 *
 */
public final class MaxUtils {
  
  public static long maxLength(long a, long b) {
    if (rightOf(a) > rightOf(b))
      return a;
    else
      return b;
  }
  
  public static long maxSupport(long a, long b) {
    if(leftOf(a) > leftOf(b))
      return a;
    else if (leftOf(a) < leftOf(b))
      return b;
    else
      return maxLength(a,b);
  }
  
  //Helper utilities
  public static long combine(int left, int right) {
    long result = left;
        result = (result << 32) | (long)right;
        return result;
  }
  
  public static int rightOf(long x) {
    return (int) (x & 0xffffffffL);
  }

  public static int leftOf(long x) {
    return (int) (x >> 32);
  }

}
