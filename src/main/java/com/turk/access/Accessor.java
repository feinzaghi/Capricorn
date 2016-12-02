package com.turk.access;

public abstract interface Accessor
{
	public abstract boolean validate();

	public abstract void doReady()
    	throws Exception;

	public abstract void doStart()
		throws Exception;

	public abstract boolean doBeforeAccess()
		throws Exception;

	public abstract boolean access()
		throws Exception;

	public abstract void parse(char[] paramArrayOfChar, int paramInt)
		throws Exception;

	public abstract boolean doAfterAccess()
    	throws Exception;

	public abstract void doFinishedAccess()
		throws Exception;

	public abstract void doSqlLoad()
    	throws Exception;

	public abstract void doFinished()
    	throws Exception;
}