package com.turk.DataImport;

/**
 * Copyright (C) 2011 UTL
 * 版权所有。 
 *
 * 文件名：ColumnObject.java
 * 文件功能描述：导入数据的列对象
 * 
 * 创建日期：
 *
 * 修改日期：
 * 修改描述：
 *
 * 修改日期：
 * 修改描述：
 */
public class ColumnObject {
	
	/**
	 * 导入数据的列对象
	 * @param columnName 列名
	 * @param dataType 数据类型
	 * @param dataLength 数据长度
	 * @param dataFormat 数据格式
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
	 * 列名称
	 */
	public String ColumnName = "";
	
	/**
	 * 数据类型 0,1标识数字、字符串，3标识时间
	 */
	public int DataType = 0;
	
	/**
	 * 数据长度
	 */
	public int DataLength = 0;
	
	/**
	 * 数据格式
	 */
	public String DataFormat = "";

}
