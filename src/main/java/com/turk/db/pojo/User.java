package com.turk.db.pojo;

public class User
{
	private int id;
	private String userName;
	private String userPwd;
	private UserGroup group;

	public UserGroup getGroup()
	{
		return this.group;
	}

	public User()
	{
	}

	public User(int id, String userName, String userPwd, int groupID)
	{
		this.id = id;
		this.userName = userName;
		this.userPwd = userPwd;
		this.group.setId(groupID);
	}

	public int getId()
	{
		return this.id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	public String getUserName()
	{
		return this.userName;
	}

	public void setUserName(String userName)
	{
		this.userName = userName;
	}

	public String getUserPwd()
	{
		return this.userPwd;
	}

	public void setUserPwd(String userPwd)
	{
		this.userPwd = userPwd;
	}

	public int getGroupID()
	{
		return this.group.getId();
	}

	public void setGroupID(int groupID)
	{
		if (this.group == null)
			this.group = new UserGroup();
		this.group.setId(groupID);
	}
	
	public String getGroupName()
	{
		return this.group.getName();
	}

	public void setGroup(UserGroup group)
	{
		this.group = group;
	}
}