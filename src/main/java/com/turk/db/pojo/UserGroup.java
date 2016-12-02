package com.turk.db.pojo;

import java.util.List;

public class UserGroup
{
	private int id;
	private String name;
	private String ids;
	private String note;
	private List<User> users;

	public UserGroup()
	{
	}

	public UserGroup(int id, String name, String ids, String note)
	{
		this.id = id;
		this.name = name;
		this.ids = ids;
		this.note = note;
	}

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

	public String getIds()
	{
		return this.ids;
	}

	public void setIds(String ids)
	{
		this.ids = ids;
	}

	public String getNote()
	{
		return this.note;
	}

	public void setNote(String note)
	{
		this.note = note;
	}

	public List<User> getUsers()
	{
		return this.users;
	}

	public void setUsers(List<User> users)
	{
		this.users = users;
	}

	public boolean equals(Object obj)
	{
		if (!(obj instanceof UserGroup))
			return false;
		UserGroup g = (UserGroup)obj;
		return g.getId() == this.id;
	}
}