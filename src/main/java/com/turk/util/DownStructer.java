package com.turk.util;

import java.util.ArrayList;
import java.util.List;

public class DownStructer {
	
	private List<String> suc;
	private List<String> fail;
	private List<String> localFail;

	public DownStructer()
	{
		this.suc = new ArrayList();
		this.fail = new ArrayList();
		this.localFail = new ArrayList();
	}

	/**
	 * �ɹ�
	 * @return
	 */
	public List<String> getSuc()
	{
		return this.suc;
	}

	/**
	 * �ɹ�
	 * @param suc
	 */
	public void setSuc(List<String> suc)
	{
		this.suc = suc;
	}

	/**
	 * ʧ��
	 * @return
	 */
	public List<String> getFail()
	{
		return this.fail;
	}

	/**
	 * ʧ��
	 * @param fail
	 */
	public void setFail(List<String> fail)
	{
		this.fail = fail;
	}

	/**
	 * ����ʧ��
	 * @return
	 */
	public List<String> getLocalFail()
	{
		return this.localFail;
	}
	
	/**
	 * ����ʧ��
	 * @param localFail
	 */
	public void setLocalFail(List<String> localFail)
	{
		this.localFail = localFail;
	}
}
