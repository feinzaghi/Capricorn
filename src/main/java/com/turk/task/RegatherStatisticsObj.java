package com.turk.task;

public class RegatherStatisticsObj
{
  private long key;
  private int times;

  public RegatherStatisticsObj()
  {
  }

  public RegatherStatisticsObj(long key, int times)
  {
    this.key = key;
    this.times = times;
  }

  public long getKey()
  {
    return this.key;
  }

  public void setKey(long key)
  {
    this.key = key;
  }

  public int getTimes()
  {
    return this.times;
  }

  public void setTimes(int times)
  {
    this.times = times;
  }
}