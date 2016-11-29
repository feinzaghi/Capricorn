package com.turk.util.file;

public abstract interface ProgressListener
{
  public abstract void finished();

  public abstract void message(String paramString);

  public abstract void progress(long paramLong1, long paramLong2);
}