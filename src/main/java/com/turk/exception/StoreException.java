package com.turk.exception;

public class StoreException extends Exception
{
  private static final long serialVersionUID = -4972255405605742292L;

  public StoreException()
  {
  }

  public StoreException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public StoreException(String message)
  {
    super(message);
  }

  public StoreException(Throwable cause)
  {
    super(cause);
  }
}