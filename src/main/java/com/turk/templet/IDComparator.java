package com.turk.templet;

import java.util.Comparator;

public class IDComparator
  implements Comparator<Integer>
{
  public int compare(Integer o1, Integer o2)
  {
    return o1.compareTo(o2);
  }
}