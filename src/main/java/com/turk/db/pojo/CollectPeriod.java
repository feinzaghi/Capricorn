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
				return new CollectPeriod(1, "һֱ");
			case 2:
				return new CollectPeriod(2, "��");
			case 3:
				return new CollectPeriod(3, "Сʱ");
			case 4:
				return new CollectPeriod(4, "��Сʱ");
			case 5:
				return new CollectPeriod(5, "15����");
			case 6:
				return new CollectPeriod(6, "4Сʱ");
			case 7:
				return new CollectPeriod(7, "5����");
			case 8:
				return new CollectPeriod(8, "12Сʱ");
		}
		return new CollectPeriod(-1, "δ֪");
	}
}