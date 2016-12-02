package com.turk.delayprobe;

public class DataEntry
{
	private String name;
	private long size;

	public DataEntry()
	{
	}

	public DataEntry(String name, long size)
	{
		this.name = name;
		this.size = size;
	}

	public DataEntry(String name)
	{
		this.name = name;
	}

	public String getName()
	{
		return this.name;
	}

	public long getSize()
	{
		return this.size;
	}

	public void setSize(long size)
	{
		this.size = size;
	}

	public boolean equals(Object obj)
	{
		DataEntry e = (DataEntry)obj;
		return getName().equals(e.getName());
	}

	public String toString()
	{
		return "[文件名:" + this.name + "  大小:" + this.size + "]";
	}
}