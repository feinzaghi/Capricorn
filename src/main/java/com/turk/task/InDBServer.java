package com.turk.task;

/**
 * ������ݿ�����
 * @author Turk
 *
 */
public class InDBServer {
	private String _indbserver;
	private String _indbuser;
	private String _inpassword;
	
	/**
	 * ������ݿ�����
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
	 * ������ݿ��û�
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
	 * ������ݿ��½����
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
