package com.turk.store;

import com.turk.exception.StoreException;

public abstract interface Store
{
	public abstract void open()
    	throws StoreException;

	public abstract void write(String paramString)
    	throws StoreException;

	public abstract void flush()
    	throws StoreException;

	public abstract void commit()
    	throws StoreException;

	public abstract void close();
}