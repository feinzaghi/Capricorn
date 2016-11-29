package com.turk.util;

public class Result
{
	private int code;
	private String detailMessage;
	private Result cause = this;

	public Result(int code)
	{
		this.code = code;
	}

	public Result(int code, String message)
	{
		this.code = code;
    	this.detailMessage = message;
	}

	public Result(int code, String message, Result cause)
	{
		this.code = code;
		this.detailMessage = message;
		this.cause = cause;
	}

	public Result(Result cause)
	{
		this.code = (cause == null ? 0 : cause.getCode());
		this.detailMessage = (cause == null ? null : cause.toString());
		this.cause = cause;
	}

	public int getCode()
	{
		return this.code;
	}

	public void setCode(int code)
	{
		this.code = code;
	}

	public String getMessage()
	{
		return this.detailMessage;
	}

	public void setMessage(String message)
	{
		this.detailMessage = message;
	}
	
	public Result getCause()
	{
		return this.cause == this ? null : this.cause;
	}

	public void setCause(Result cause)
	{
		this.cause = cause;
	}

	public String toString()
	{
		String s = getClass().getName();
		int code = getCode();
		String message = getMessage();
		return s + ":" + 
		code;
	}
}