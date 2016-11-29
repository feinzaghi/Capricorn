package com.turk.DataImport;

import com.turk.DataImport.ColumnObject;

/**
 * Copyright (C) 2011 UTL
 * ��Ȩ���С� 
 *
 * �ļ�����IOutput.java
 * �ļ�������������������ӿ�
 * 
 * �������ڣ�
 *
 * �޸����ڣ�
 * �޸�������
 *
 * �޸����ڣ�
 * �޸�������
 */
public interface IOutput {
	
	public void setExecuteImmediate(boolean ExecuteImmediate);
	
	/**
	 * ִ�����
	 * @param cltFileName  �����ļ���
	 * @param columns  ����м���
	 * @param tableName ������
	 * @param fileName  ��������ļ���
	 * @param timeString  �ļ�ʱ���ַ���
	 */
	public void ExcuteImport(int KeyID,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit);
	
	
	/**
	 * ִ�����
	 * @param DBServer
	 * @param userid
	 * @param password
	 * @param cltFileName
	 * @param columns
	 * @param tableName
	 * @param fileName
	 * @param timeString
	 */
	public void ExcuteImport(int KeyID,String DBServer,String userid,String password,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit);
	
}
