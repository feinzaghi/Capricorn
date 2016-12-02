package com.turk.parser.xparser;

public class IllegalTagException extends Exception
{
	private static final long serialVersionUID = -7435669314371760771L;

	public IllegalTagException()
	{
	}

	public IllegalTagException(String message)
	{
		super(message);
	}

	public IllegalTagException(Throwable cause)
	{
		super(cause);
	}

	public IllegalTagException(String message, Throwable cause)
	{
		super(message, cause);
	}
}