package com.turk.exception;

public class ParseException extends Exception
{
  private static final long serialVersionUID = -4607404540270646338L;

  public ParseException()
  {
  }

  public ParseException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public ParseException(String message)
  {
    super(message);
  }

  public ParseException(Throwable cause)
  {
    super(cause);
  }
}