package com.turk.bean;

import java.util.HashMap;
import java.util.Map;

class PBeanContainer<T extends PBean>
{
	Map<Integer, T> beans;

	public PBeanContainer()
	{
		this.beans = new HashMap<Integer, T>();
	}

	public String getBeanByID(int id)
	{
		String s = null;
		if (this.beans.containsKey(Integer.valueOf(id)))
		{
			PBean bean = (PBean)this.beans.get(Integer.valueOf(id));
			if (bean != null) {
				s = bean.getBean();
			}
		}
		return s;
	}

	public void add(T bean)
  	{
		this.beans.put(Integer.valueOf(bean.getId()), bean);
  	}
}