package com.turk.DataImport;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.CommonDataSource;

import org.apache.log4j.Logger;






import com.turk.Config.ConstDef;
import com.turk.Config.SystemConfig;
import com.turk.DataImport.ColumnObject;
import com.turk.util.BalySqlloadThread;
import com.turk.util.DBLogger;
import com.turk.util.LogMgr;
import com.turk.util.SqlldrResult;
import com.turk.util.ThreadPool;
import com.turk.util.Util;
import com.turk.util.loganalyzer.SqlLdrLogAnalyzer;

/**
 * Copyright (C) 2011 UTL
 * 版权所有。 
 *
 * 文件名：SqlLoadImport.java
 * 文件功能描述：SQL LOAD入库类
 * 
 * 创建日期：
 *
 * 修改日期：
 * 修改描述：
 *
 * 修改日期：
 * 修改描述：
 */
public class SqlLoadImport implements IOutput{
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	private boolean executeimmediate = false;
	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();
	//执行文件入库
	public void ExcuteImport(int KeyID,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit)
	{
		
		try
		{
			//控制文件流
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(cltFileName, false));

	
			String charSet = SystemConfig.getInstance().getSqlldrCharset();
			if ((Util.isOracle()))
			{
				bw.write("load data\r\n");

				if ((charSet != null) && (charSet.length() > 0))
					bw.write("CHARACTERSET " + charSet + " \r\n");
				else {
					bw.write("CHARACTERSET AL32UTF8 \r\n");
				}
				bw.write("infile '" + fileName + "' ");
				bw.write("append into table " + tableName + " \r\n");
				bw.write("FIELDS TERMINATED BY \""+strSplit+"\"\r\n");
				bw.write("TRAILING NULLCOLS\r\n");
				bw.write("(");
			
				int i = 0;
				for (ColumnObject column : columns)
				{
						switch (column.DataType)
						{
							case 0:
							case 1:
								bw.write(column.ColumnName);
								break;
							case 2:
								bw.write(column.ColumnName + " CHAR(" + 
										column.DataFormat + ")");
								break;
							case 3:
								bw.write(column.ColumnName + 
								" Date 'YYYY-MM-DD HH24:MI:SS'");
								break;
							case 4:
								bw.write(column.ColumnName + 
								" LOBFILE(LOBF_00006) TERMINATED BY EOF ");
						}

						if (column.DataLength > 0)
						{
							bw.write(" CHAR(" + column.DataLength + ")");
						}
						if (i < columns.length -1) {
							bw.write(",");
						}
						i = i + 1;
				}
				
				

				bw.write(")\r\n");
				bw.close();

				
				RunSqlLoad(KeyID,tableName, fileName, cltFileName, timeString);
			}

			
				/*
				File logFile = new File(SystemConfig.getInstance().getCurrentPath() + File.separatorChar + 
						"ldrlog", timeString + "_" + tableName + ".log");
				if (logFile.exists())
					logFile.delete();*/
			
		}
		catch (Exception e)
		{
			this.log.error("BuildSQLLdr", e);
		}
		
	}
	
	public void ExcuteImport(int KeyID,String DBServer,String userid,String password,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit)
	{
		try
		{
			//控制文件流
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(cltFileName, false));

			//写控制文件
			String charSet = SystemConfig.getInstance().getSqlldrCharset();
			if ((Util.isOracle()))
			{
				bw.write("load data\r\n");

				if ((charSet != null) && (charSet.length() > 0))
					bw.write("CHARACTERSET " + charSet + " \r\n");
				else {
					bw.write("CHARACTERSET AL32UTF8 \r\n");
				}
				bw.write("infile '" + fileName + "' ");
				bw.write("append into table " + tableName + " \r\n");
				bw.write("FIELDS TERMINATED BY \""+strSplit+"\"\r\n");
				bw.write("TRAILING NULLCOLS\r\n");
				bw.write("(");
			
				int i = 0;
				for (ColumnObject column : columns)
				{
						switch (column.DataType)
						{
							case 0:
							case 1:
								bw.write(column.ColumnName);
								break;
							case 2:
								bw.write(column.ColumnName + " CHAR(" + 
										column.DataFormat + ")");
								break;
							case 3:
								bw.write(column.ColumnName + 
									" Date 'YYYY-MM-DD HH24:MI:SS'");
								break;
							case 4:
								bw.write(column.ColumnName + 
								" LOBFILE(LOBF_00006) TERMINATED BY EOF ");
							case 5:
								bw.write(column.ColumnName + 
									" Timestamp 'YYYY-MM-DD HH24:MI:SS.FF3'");
								break;
						}

						if (column.DataLength > 0)
						{
							bw.write(" CHAR(" + column.DataLength + ")");
						}
						if (i < columns.length -1) {
							bw.write(",");
						}
						i = i + 1;
				}
				
				

				bw.write(")\r\n");
				bw.close();

				
				RunSqlLoad(KeyID,DBServer,userid,password,tableName, 
						fileName, cltFileName, timeString);
			}
		}
		catch (Exception e)
		{
			this.log.error("BuildSQLLdr", e);
		}
	}
	
	/**
	 * 
	 * @param DBServer
	 * @param userid
	 * @param password
	 * @param strTable
	 * @param txtFile
	 * @param ctlFile
	 * @param timeString
	 */
	private void RunSqlLoad(int KeyID,String DBServer,String userid,String password,
			String strTable, String txtFile,String ctlFile,String timeString)
	{
		String strOracleBase = DBServer;
		String strOracleUserName = userid;
		String strOraclePassword = password;
		SqlLoadCommonFunction(KeyID,strTable,txtFile,ctlFile,timeString,
				strOracleBase,strOracleUserName,strOraclePassword);
	}
	
	/**
	 * SQLLoad入库
	 * @param strTable
	 * @param txtFile
	 * @param ctlFile
	 */
	private void RunSqlLoad(int KeyID,String strTable, String txtFile,String ctlFile,String timeString)
	{
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();
		SqlLoadCommonFunction(KeyID,strTable,txtFile,ctlFile,timeString,strOracleBase,strOracleUserName,strOraclePassword);
		
	}
	
	/**
	 * 入库公共方法
	 * @param KeyID
	 * @param strTable
	 * @param txtFile
	 * @param ctlFile
	 * @param timeString
	 * @param strOracleBase
	 * @param strOracleUserName
	 * @param strOraclePassword
	 */
	private void SqlLoadCommonFunction(int KeyID,String strTable, String txtFile,String ctlFile,String timeString,
			String strOracleBase,String strOracleUserName,String strOraclePassword)
	{
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		
		//日志文件名
		String logFileName = txtFile.substring(txtFile.lastIndexOf(File.separator),txtFile.lastIndexOf("."));
		logFileName = strCurrentPath + File.separatorChar + "ldrlog" + 
		File.separatorChar  + logFileName + ".log";
		
		File logpath = new File(strCurrentPath + File.separatorChar + "ldrlog");
		if(!logpath.isDirectory())
		{
			ConstDef.CreateFolder(strCurrentPath + File.separatorChar,"ldrlog");
		}
		//错误文件文件名
		String badFileName = txtFile.substring(txtFile.lastIndexOf(File.separator),txtFile.lastIndexOf("."));
		badFileName = strCurrentPath + File.separatorChar + "ldrlog" + 
		File.separatorChar + badFileName + ".bad";
		//  21971520
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=0 control=%s " +
				"bad=%s log=%s rows=500 " + SystemConfig.getInstance().getreadsize() + " errors=9999", 
				new Object[] { strOracleUserName, strOraclePassword, 
				strOracleBase, ctlFile, badFileName, logFileName });
		
	
		try
		{
			BalySqlloadThread sqlthread = new BalySqlloadThread();

			this.log.debug("Input Database: " + strTable + "--" + cmd);
			
			sqlthread.setExeccmd(cmd);
			sqlthread.setLogFileName(logFileName);
			sqlthread.setTxtFileName(txtFile);
			sqlthread.setKeyID(KeyID);
			sqlthread.setTimeString(timeString);
			sqlthread.setCltFileName(ctlFile);
			sqlthread.setExecuteImmediate(executeimmediate);
			ThreadPool.getInstance().addTask(sqlthread.taskCore());
			/* 2012/10/24 屏蔽，测试线程入库
			int retCode = sqlthread.runcmd(cmd);
			if ((retCode == 0) || (retCode == 2))
			{
				this.log.debug("路测: " + strTable + ": sqldr OK. retCode=" + 
						retCode);
				if(retCode == 0)
					IsSuccess = true;
				
				if(retCode == 2)
				{
					SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
					SqlldrResult result = analyzer.analysis(new FileInputStream(logFileName));
					this.log.debug("路测: " + strTable + ":Sqldr retCode = 2;" 
							+ "Msg=" + result.getMessage());
					//对于SQL返回信息retCode=2的信息，将日志文件记录下来。
					Util.FileCopy(logFileName, logFileName + ".err");
				}
			}
			else if ((retCode != 0) && (retCode != 2))
			{
				int maxTryTimes = 3;
				int tryTimes = 0;
				long waitTimeout = 30000L;
				while (tryTimes < maxTryTimes)
				{
					retCode = sqlthread.runcmd(cmd);
					if ((retCode == 0) || (retCode == 2))
					{
						break;
					}

					tryTimes++;
					waitTimeout *= 2L;

					this.log.error("路测: " + strTable  + ": 第" + tryTimes + 
							"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode);

					Thread.currentThread(); 
					Thread.sleep(waitTimeout);
				}

				if ((retCode == 0) || (retCode == 2))
				{
					this.log.info("路测: " + strTable  + ": " + tryTimes + 
							"次Sqlldr尝试入库后成功. retCode=" + retCode);
				}
				else
				{
					this.log.error("路测: " + strTable  + " : " + tryTimes + 
							"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode);

				}
			}
			else
			{
				this.log.error("路测: " + strTable  + ": sqlldr 失败 并且不重试.");
			}*/
		}
		catch (Exception e)
		{
			this.log.error("路测: " + strTable  + ": sqlldr exception. " + cmd, e);
		}

		
		//日志分析
		/* 2012/10/24 屏蔽，测试线程入库
		File logFile = new File(logFileName);
		if ((!logFile.exists()) || (!logFile.isFile()))
		{
			this.log.info("路测: " + strTable + ": " + logFileName + "不存在.");
			return;
		}
		
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try
		{
			SqlldrResult result = analyzer.analysis(new FileInputStream(logFileName));
			if (result == null) {
				return;
			}
			
			File file = new File(txtFile);
			double fileBytes = 0;
			if (file.exists()) { 
				FileInputStream fis = null; 
				fis = new FileInputStream(file); 
				fileBytes = fis.available(); 
				fileBytes = (double)fileBytes/(double)1024;
				
				fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
				fis.close();
			}
			
			
			this.log.debug("路测: " + strTable + ": SQLLDR日志分析结果:  表名=" + 
					strTable + " 数据时间=" + 
					timeString + "文件大小=" + fileBytes + "KB 文件入库时长" + result.getRunTime() +
					"(s) 入库成功条数=" + result.getLoadSuccCount() + " sqlldr日志=" + 
					logFileName);
			
			
			dbLogger.log(KeyID, result.getTableName(), 
					timeString, 
					result.getLoadSuccCount(), KeyID,result.getRunTime(),fileBytes);
			if(IsSuccess)
			{
				//完成入库，日志分析后，删除日志文件
				if (SystemConfig.getInstance().isDeleteLog())
				{
					if (logFile.exists())
						logFile.delete();
				}
			}
			
		}
		catch (Exception e)
		{
			this.log.error("路测: " + strTable + ": sqlldr日志分析失败，文件名：" + 
					logFileName + "，原因: ", e);
		}*/
	}

	@Override
	public void setExecuteImmediate(boolean ExecuteImmediate) {
		// TODO Auto-generated method stub
		this.executeimmediate = ExecuteImmediate;
	}
}
