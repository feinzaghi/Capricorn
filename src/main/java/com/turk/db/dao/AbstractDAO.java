package com.turk.db.dao;

import java.util.List;
import org.apache.log4j.Logger;
import com.turk.util.LogMgr;

public class AbstractDAO<T>
  	implements DAO<T>
{
	protected static Logger logger = LogMgr.getInstance().getSystemLogger();

	public int add(T entity)
	{
		return 0;
	}

	public boolean delete(T entity)
	{
		return false;
	}

	public boolean delete(int id)
	{
		return false;
	}

	public T getById(int id)
	{
		return null;
	}

	public T getByName(String name)
	{
		return null;
	}

	public List<T> list()
	{
		return null;
	}

	public PageQueryResult<T> pageQuery(int pageSize, int currentPage)
	{
		return null;
	}

	public List<T> query(String sql)
	{
		return null;
	}

	public boolean update(T entity)
	{
		return false;
	}

	public boolean validate(T entity)
	{
		return false;
	}

	public int clearAll()
	{
		return 0;
	}

	public boolean exists(T entity)
	{
		return false;
	}

	public List<T> criteriaQuery(T dev)
	{
		return null;
	}
}