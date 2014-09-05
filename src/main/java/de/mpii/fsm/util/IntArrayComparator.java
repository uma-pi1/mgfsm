package de.mpii.fsm.util;

public class IntArrayComparator {

  public static int compare(int[] array1, int[] array2, int len1, int len2) {

    if (len1 > len2) return 1;
    if (len1 < len2) return -1;

    // arrays of the same size check elements one by one
    for (int a1 = 0; a1 < len1; a1++) {
      if (array1[a1] == array2[a1]) {
        continue;
      } else {
        return array1[a1] - array2[a1];
      }

    }
    return 0;

  }

}
