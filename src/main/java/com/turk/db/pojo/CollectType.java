package com.turk.db.pojo;

/**
 * �ɼ�����
 * @author Administrator
 *
 */
public class CollectType
{
	private int value;
	private String name;

	public CollectType()
	{
	}

	public CollectType(int value, String name)
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

	public static CollectType create(int value)
	{
		switch (value)
		{
			case 1:
				return new CollectType(1, "telnet");
			case 2:
				return new CollectType(2, "tcp");
			case 3:
				return new CollectType(3, "ftp");
			case 4:
				return new CollectType(4, "�����ļ�");
			case 5:
				return new CollectType(5, "���ݿ�");
			case 60:
				return new CollectType(5, "���ݿ�(��Ա�)");
		}
		return new CollectType(-1, "δ֪");
	}
}