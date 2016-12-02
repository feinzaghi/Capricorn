package com.turk.collect;

import java.util.ArrayList;
import java.util.List;

/**
 * 采集下载结构
 * @author Administrator
 *
 */
public class DownStructer
{
	private List<String> suc;
	private List<String> fail;
	private List<String> localFail;

	public DownStructer()
	{
		this.suc = new ArrayList<String>();
		this.fail = new ArrayList<String>();
		this.localFail = new ArrayList<String>();
	}

	/**
	 * 成功
	 * @return
	 */
	public List<String> getSuc()
	{
		return this.suc;
	}

	/**
	 * 成功
	 * @param suc
	 */
	public void setSuc(List<String> suc)
	{
		this.suc = suc;
	}

	/**
	 * 失败
	 * @return
	 */
	public List<String> getFail()
	{
		return this.fail;
	}

	/**
	 * 失败
	 * @param fail
	 */
	public void setFail(List<String> fail)
	{
		this.fail = fail;
	}

	/**
	 * 本地失败
	 * @return
	 */
	public List<String> getLocalFail()
	{
		return this.localFail;
	}
	
	/**
	 * 本地失败
	 * @param localFail
	 */
	public void setLocalFail(List<String> localFail)
	{
		this.localFail = localFail;
	}
}