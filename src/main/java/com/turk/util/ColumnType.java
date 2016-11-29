package com.turk.util;

public class ColumnType
{
	private String columnName;
	private String type;
	private String format;

	public ColumnType()
	{
	}

	public ColumnType(String type, String format, String cn)
	{
		this.type = type;
		this.format = format;
		this.columnName = cn;
	}

	public String getColumnName()
	{
		return this.columnName;
	}

	public void setColumnName(String columnName)
	{
		this.columnName = columnName;
	}

	public String getType()
	{
		return this.type;
	}

	public void setType(String type)
	{
		this.type = type;
	}

	public String getFormat()
	{
		return this.format;
	}

	public void setFormat(String format)
	{
		this.format = format;
	}

	public boolean equals(Object obj)
	{
		if (obj == this) 
			return true;

		if ((obj instanceof ColumnType))
		{
			ColumnType ct = (ColumnType)obj;

			return (ct.getColumnName().equals(this.columnName)) && 
			(ct.getFormat().equals(this.format)) && 
			(ct.getType().equals(this.type));
		}
		return false;
	}
}