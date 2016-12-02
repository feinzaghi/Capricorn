package com.turk.exception;

public class InvalidParameterValueException extends Exception
{
  private static final long serialVersionUID = -4221202827734871368L;

  public InvalidParameterValueException()
  {
  }

  public InvalidParameterValueException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public InvalidParameterValueException(String message)
  {
    super(message);
  }

  public InvalidParameterValueException(Throwable cause)
  {
    super(cause);
  }
}