package com.turk.framework;

public class SystemError
{
	private String code;
	private String des;
	private String cause;
	private String action;

	public SystemError()
	{
	}

	public SystemError(String code, String des, String cause, String action)
	{
		this.code = code;
		this.des = des;
		this.cause = cause;
		this.action = action;
	}

	public String getCode()
	{
		return this.code;
	}

	public void setCode(String code)
	{
		this.code = code;
	}

	public String getDes()
	{
		return this.des;
	}

	public void setDes(String des)
	{
		this.des = des;
	}

	public String getCause()
	{
		return this.cause;
	}

	public void setCause(String cause)
	{
		this.cause = cause;
	}	

	public String getAction()
	{
		return this.action;
	}

	public void setAction(String action)
 	{
		this.action = action;
 	}
}