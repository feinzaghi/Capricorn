package com.turk.DataImport;

import com.turk.DataImport.ColumnObject;

/**
 * Copyright (C) 2011 UTL
 * 版权所有。 
 *
 * 文件名：IOutput.java
 * 文件功能描述：数据输出接口
 * 
 * 创建日期：
 *
 * 修改日期：
 * 修改描述：
 *
 * 修改日期：
 * 修改描述：
 */
public interface IOutput {
	
	public void setExecuteImmediate(boolean ExecuteImmediate);
	
	/**
	 * 执行入库
	 * @param cltFileName  控制文件名
	 * @param columns  入库列集合
	 * @param tableName 入库表名
	 * @param fileName  入库数据文件名
	 * @param timeString  文件时间字符串
	 */
	public void ExcuteImport(int KeyID,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit);
	
	
	/**
	 * 执行入库
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
