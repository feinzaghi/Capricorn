package com.turk.DataImport;

/**
 * Copyright (C) 2011 UTL
 * ��Ȩ���С� 
 *
 * �ļ�����ColumnObject.java
 * �ļ������������������ݵ��ж���
 * 
 * �������ڣ�
 *
 * �޸����ڣ�
 * �޸�������
 *
 * �޸����ڣ�
 * �޸�������
 */
public class ColumnObject {
	
	/**
	 * �������ݵ��ж���
	 * @param columnName ����
	 * @param dataType ��������
	 * @param dataLength ���ݳ���
	 * @param dataFormat ���ݸ�ʽ
	 */
	public ColumnObject(String columnName,int dataType
			,int dataLength,String dataFormat )
	{
		this.ColumnName = columnName;
		this.DataType = dataType;
		this.DataLength = dataLength;
		this.DataFormat = dataFormat;
	}
	
	/**
	 * ������
	 */
	public String ColumnName = "";
	
	/**
	 * �������� 0,1��ʶ���֡��ַ�����3��ʶʱ��
	 */
	public int DataType = 0;
	
	/**
	 * ���ݳ���
	 */
	public int DataLength = 0;
	
	/**
	 * ���ݸ�ʽ
	 */
	public String DataFormat = "";

}
