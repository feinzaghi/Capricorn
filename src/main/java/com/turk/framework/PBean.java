package com.turk.framework;

import java.util.HashMap;
import java.util.Map;

import com.turk.util.Param;

/**
 * PBean 结构对象
 * @author Administrator
 *
 */
class PBean
{
	int id;
	String name;
	String des;
	String bean;
	Map<String, Param> params;

	public PBean()
  	{
		this.params = new HashMap<String, Param>();
  	}

	/**
	 * ID
	 * @return
	 */
	public int getId()
	{
		return this.id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	/**
	 * 名称
	 * @return
	 */
	public String getName()
	{
		return this.name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	/**
	 * 描述
	 * @return
	 */
	public String getDes()
	{	
		return this.des;
	}

	public void setDes(String des)
	{
		this.des = des;
	}

	/**
	 * 反射对象名称
	 * @return
	 */
	public String getBean()
	{
		return this.bean;
	}

	public void setBean(String bean)
	{
		this.bean = bean;
	}

	public void addParam(Param p)
	{
		if ((p == null) || (p.getName() == null)) {
			return;
		}
		this.params.put(p.getName(), p);
	}
}