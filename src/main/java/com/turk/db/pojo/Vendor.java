package com.turk.db.pojo;

public class Vendor
{
	private int id;
	private String nameCH;
	private String nameEN;

	public Vendor()
	{
	}

	public Vendor(int id, String nameCH, String nameEN)
	{
		this.id = id;
		this.nameCH = nameCH;
		this.nameEN = nameEN;
	}

	public int getId()
	{
		return this.id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	public String getNameCH()
	{
		return this.nameCH;
	}

	public void setNameCH(String nameCH)
	{
		this.nameCH = nameCH;
	}

	public String getNameEN()
	{
		return this.nameEN;
	}

	public void setNameEN(String nameEN)
	{
		this.nameEN = nameEN;
	}
}