package com.turk.templet;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class Table
{
	private int id;
	private String name;
	private String splitSign = ";";

	private Map<Integer, Column> columns = new TreeMap(new IDComparator());
	private static final String ORACLE_KEYWORD = "Mode,";

	public int getId()
	{
		return this.id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	public String getName()
	{
		return this.name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getSplitSign()
	{
		return this.splitSign;
	}

	public void setSplitSign(String splitSign)
	{
		this.splitSign = splitSign;
	}

	public Map<Integer, Column> getColumns()
	{
		return this.columns;
	}

	public void setColumns(Map<Integer, Column> columns)
	{
		this.columns = columns;
	}

	public String listColumnNames(String splitSign)
	{
		StringBuilder sb = new StringBuilder();
		Collection<Column> cols = this.columns.values();
		for (Column col : cols)
		{
			sb.append(wrapName(col.getName())).append(splitSign);
		}

		return sb.toString();
	}

	private String wrapName(String name)
	{
		if ("Mode,".indexOf(name) != -1) {
			name = "\"" + name + "\"";
		}
		return name;
	}

	public String listColumnNamesWithType(String splitSign)
  	{
		StringBuilder sb = new StringBuilder();
		Collection<Column> cols = this.columns.values();
		int index = 0;
		for (Column col : cols)
		{
			String colName = wrapName(col.getName());
			int type = col.getType();
			String format = col.getFormat();
			if (type == 2)
			{
				sb.append(colName).append(" CHAR(").append(format).append(") ").append(splitSign);
			}
			else if (type == 3)
			{
				sb.append(colName).append(" Date '").append(format).append("'").append(splitSign);
			}
			else if (type == 4)
			{
				String filler = "filler_" + index++;
				sb.append(filler).append(" filler char(9999999)").append(splitSign).append(colName).append(" LOBFILE(" + 
						filler + ") TERMINATED BY EOF ").append(splitSign);
			}
			else
			{
				sb.append(colName).append(splitSign);
			}
		}
		return sb.toString();
  	}

	public class Column
	{
		private String name;
		private int index;
		private int type;
		private String format;
		private int srcIndex;
		private int srcType;

		public Column() {
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

		public int getIndex()
		{
			return this.index;
		}

		public void setIndex(int index)
		{
			this.index = index;
		}

		public int getType()
		{
			return this.type;
		}

		public void setType(int type)
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

		public int getExtIndex()
		{
			return this.srcIndex;
		}
		
		public void setExtIndex(int extIndex)
		{
			this.srcIndex = extIndex;
		}

		public int getSrcType()
		{
			return this.srcType;
		}

		public void setSrcType(int srcType)
		{
			this.srcType = srcType;
		}
	}
}