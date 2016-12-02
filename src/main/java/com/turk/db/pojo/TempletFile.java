package com.turk.db.pojo;

public class TempletFile
{
	private String name;
	private long size;
	private String modifyDate;
	private String content;

	public TempletFile()
	{
	}

	public TempletFile(String name, long size, String modifyDate, String content)
	{
		this.name = name;
		this.size = size;
		this.modifyDate = modifyDate;
		this.content = content;
	}

	public String getName()
	{
		return this.name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public long getSize()
	{
		return this.size;
	}

	public void setSize(long size)
	{
		this.size = size;
	}

	public String getModifyDate()
	{
		return this.modifyDate;
 	}

	public void setModifyDate(String modifyDate)
	{
		this.modifyDate = modifyDate;
  	}

	public String getContent()
	{
		return this.content;
	}

	public void setContent(String content)
	{
		this.content = content;
	}
}