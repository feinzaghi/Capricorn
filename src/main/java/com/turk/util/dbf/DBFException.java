package com.turk.util.dbf;

import java.io.IOException;

public class DBFException extends IOException
{
  private static final long serialVersionUID = 5961956027168324576L;

  public DBFException()
  {
  }

  public DBFException(String msg)
  {
    super(msg);
  }
}