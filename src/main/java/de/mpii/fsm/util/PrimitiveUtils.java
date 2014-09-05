package de.mpii.fsm.util;

/**
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public final class PrimitiveUtils {

    /**
     * Combines the two given int values into a single long value.
     * 
     * @param l
     * @param r
     * @return 
     */
    public static long combine(int l, int r) {
        return ((long) l << 32) | ((long) r & 0xFFFFFFFFL);
    }

    /**
     * Extracts int value corresponding to left-most 32 bits of given long value.
     * 
     * @param c
     * @return 
     */
    public static int getLeft(long c) {
        return (int) (c >> 32);
    }

    /**
     * Extracts int value corresponding to right-most 32 bits of given long value.
     * 
     * @param c
     * @return 
     */
    public static int getRight(long c) {
        return (int) c;
    }
}
