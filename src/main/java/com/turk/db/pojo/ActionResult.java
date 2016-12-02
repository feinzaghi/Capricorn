package com.turk.db.pojo;

import com.turk.framework.SystemError;

public class ActionResult
{
	private SystemError error;
	private String forwardURL;
	private String returnURL;
	private Object data;
	private Object wparam;
	private Object lparam;

	public ActionResult(SystemError error, String forwordURL, String returnURL, Object data)
	{
		this.error = error;
		this.forwardURL = forwordURL;
    	this.returnURL = returnURL;
    	this.data = data;
	}

	public ActionResult()
	{
	}

	public SystemError getError()
	{
		return this.error;
	}

	public void setError(SystemError error)
	{
		this.error = error;
	}

	public String getForwardURL()
	{
		return this.forwardURL;
	}

	public void setForwardURL(String forwardURL)
	{
		this.forwardURL = forwardURL;
	}

	public Object getData()
	{
		return this.data;
	}

  public void setData(Object data)
  {
    this.data = data;
  }

  	public String getReturnURL()
  	{
  		return this.returnURL;
  	}

  	public void setReturnURL(String returnURL)
  	{
  		this.returnURL = returnURL;
  	}

  	public Object getWparam()
  	{
  		return this.wparam;
  	}	

  	public void setWparam(Object wparam)
  	{
  		this.wparam = wparam;
  	}

  	public Object getLparam()
  	{
  		return this.lparam;
  	}

  	public void setLparam(Object lparam)
  	{
  		this.lparam = lparam;
  	}
}