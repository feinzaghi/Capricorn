package com.turk.task;

/**
 * 入库数据库配置
 * @author Turk
 *
 */
public class InDBServer {
	private String _indbserver;
	private String _indbuser;
	private String _inpassword;
	
	/**
	 * 入库数据库名称
	 * @return
	 */
	public String getInDBServer()
	{
		return this._indbserver;
	}
	
	public void setInDBServer(String indbserver)
	{
		_indbserver = indbserver;
	}
	
	/**
	 * 入库数据库用户
	 * @return
	 */
	public String getInDBUser()
	{
		return this._indbuser;
	}
	
	public void setInDBUser(String indbuser)
	{
		this._indbuser = indbuser;
	}
	
	/**
	 * 入库数据库登陆密码
	 * @return
	 */
	public String getInDBPassword()
	{
		return this._inpassword;
	}
	
	public void setInDBPassword(String indbpassword)
	{
		this._inpassword = indbpassword;
	}
}
