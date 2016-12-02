package com.turk.db.pojo;

public class CollectPeriod
{
	private int value;
	private String name;

	public CollectPeriod()
	{
	}

	public CollectPeriod(int value, String name)
	{
		this.value = value;
		this.name = name;
	}

	public int getValue()
	{
		return this.value;
	}

	public void setValue(int value)
	{
		this.value = value;
	}

	public String getName()
	{
		return this.name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public static CollectPeriod create(int value)
	{
		switch (value)
		{
			case 1:
				return new CollectPeriod(1, "一直");
			case 2:
				return new CollectPeriod(2, "天");
			case 3:
				return new CollectPeriod(3, "小时");
			case 4:
				return new CollectPeriod(4, "半小时");
			case 5:
				return new CollectPeriod(5, "15分钟");
			case 6:
				return new CollectPeriod(6, "4小时");
			case 7:
				return new CollectPeriod(7, "5分钟");
			case 8:
				return new CollectPeriod(8, "12小时");
		}
		return new CollectPeriod(-1, "未知");
	}
}