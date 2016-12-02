package com.turk.distributor;

import com.turk.Config.SystemConfig;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.alarm.AlarmMgr;
import com.turk.distributor.DistributeTemplet.FieldTemplet;
import com.turk.task.CollectObjInfo;
import com.turk.util.BalySqlloadThread;
import com.turk.util.CommonDB;
import com.turk.util.DBLogger;
import com.turk.util.LogMgr;
import com.turk.util.SqlldrResult;
import com.turk.util.Util;
import com.turk.util.loganalyzer.SqlLdrLogAnalyzer;
import com.turk.util.string.LevenshteinDistance;

/**
 * SQLLDR分发对象
 * @author Administrator
 *
 */
public class DistributeSqlLdr
{
	private CollectObjInfo collectInfo;
	private DistributeTemplet disTmp;
	private Logger log = LogMgr.getInstance().getSystemLogger();
  
	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public DistributeSqlLdr(CollectObjInfo ColInfo)
	{
		this.collectInfo = ColInfo;
		this.disTmp = ((DistributeTemplet)this.collectInfo.getDistributeTemplet());
	}

	private List<Integer> findDuplicateCol(List<String> cols, String col)
	{
		List<Integer> index = new ArrayList<Integer>();
		for (int i = 0; i < cols.size(); i++)
		{
			if (!((String)cols.get(i)).equalsIgnoreCase(col))
				continue;
			index.add(Integer.valueOf(i));
		}

		return index.size() > 1 ? index : null;
	}

	/**
	 * 创建SQLLDR必要条件
	 * @param tableIndex
	 * @param tempFile
	 */
	@SuppressWarnings({ "resource", "unchecked" })
	public void buildSqlLdr(int tableIndex, String tempFile)
	{
		String logStr = null;
		DistributeTemplet.TableTemplet tableInfo = (DistributeTemplet.TableTemplet)this.disTmp.tableTemplets.get(Integer.valueOf(tableIndex));
    
		String currentPath = SystemConfig.getInstance().getCurrentPath();
		String charSet = SystemConfig.getInstance().getSqlldrCharset();

		int retCode = -1;

		File txttempfile = new File(currentPath, tempFile + ".txt");
		if (!txttempfile.exists())
		{
			logStr = this.collectInfo.getSysName() + ": " + 
				txttempfile.getAbsolutePath() + " 不存在.";
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);
			return;
		}

		if (txttempfile.length() == 0L)
		{
			logStr = this.collectInfo.getSysName() + ": " + 
				txttempfile.getAbsolutePath() + " 内容为空.";
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);
			txttempfile.delete();
			return;
		}

		try
		{
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(currentPath + 
							File.separatorChar + tempFile + ".ctl", false));

			File txtFile = new File(currentPath + File.separatorChar + tempFile + ".txt");
			InputStream in = new FileInputStream(txtFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String splitSign = ";";
			String firstLine = br.readLine();
			try
			{
				String DeviceID = String.valueOf(this.collectInfo.getDevInfo().getDevID());
				splitSign = firstLine.substring(firstLine.indexOf(DeviceID) + 
						DeviceID.length(), firstLine.indexOf(DeviceID) + 
						DeviceID.length() + 1);
			}
			catch (Exception e)
			{
				logStr = "get splitSign error:" + e.getMessage();
				this.log.error(logStr);
				this.collectInfo.log("入库", logStr, e);
			}
			if (br != null)
			{
				br.close();
			}
			if (in != null)
			{
				in.close();
			}
			List<String> raws = new ArrayList<String>();
			Map<String, List<?>> colnameToIndex = new HashMap<String, List<?>>();
			if (firstLine != null)
			{
				String[] items = firstLine.split(";");
				for (String s : items)
				{
					if (!Util.isNotNull(s))
						continue;
					raws.add(s.trim());
				}

				for (String s : raws)
				{
					if (colnameToIndex.containsKey(s))
						continue;
					List<?> index = findDuplicateCol(raws, s);
					if (index == null)
						continue;
					colnameToIndex.put(s, index);
				}
			}

			if (Util.isOracle())
			{
				bw.write("load data\r\n");

				if ((charSet != null) && (charSet.length() > 0))
				{
					bw.write("CHARACTERSET " + charSet + " \r\n");
				}
				else 
				{
					bw.write("CHARACTERSET AL32UTF8 \r\n");
				}
				
				bw.write("infile '" + currentPath + File.separatorChar + 
						tempFile + ".txt' ");
				bw.write("append into table " + tableInfo.tableName + " \r\n");
				bw.write("FIELDS TERMINATED BY \";\"\r\n");
				bw.write("TRAILING NULLCOLS\r\n");
				bw.write("(");
				Object strFieldMap;
				if (this.disTmp.stockStyle == 3)
				{
					String m_RawColumnsList = ReadFileFirstLine(tempFile);
					if (m_RawColumnsList == null) {
						return;
					}

					String[] FieldMappingList = m_RawColumnsList.split(";");
					String StrNewFieldList = "";
					for (int i = 0; i < FieldMappingList.length; i++)
					{
						strFieldMap = FieldMappingList[i].trim();
						if (((String)strFieldMap).trim() == "") 
						{
							continue;
						}
						for (int j = 0; j < tableInfo.fields.size(); j++)
						{
							DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)tableInfo.fields.get(Integer.valueOf(j));
							if (!field.m_strFieldMapping.equals(strFieldMap)) 
							{
								continue;
							}
							switch (field.m_nDataType)
							{
								case 1:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + ",";
									break;
								case 2:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + ",";
									break;
								case 3:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + 
										" Date 'YYYY-MM-DD HH24:MI:SS',";
									break;
								case 4:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + 
									" LOBFILE(LOBF_00006) TERMINATED BY EOF,";
									break;
								case 5://时间格式（精确到毫秒）
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + 
									" TIMESTAMP '" + field.m_strDataTimeFormat + "'";
									break;
								default:
									break;
							}
						}
					}

					if (StrNewFieldList.length() >= 1)
						StrNewFieldList = StrNewFieldList.substring(0, StrNewFieldList.length() - 1);
					bw.write(StrNewFieldList);
				}
				else if (isCreateCtlByRawName(tableInfo.fields))
				{
					//2013-01-11 OMCID -> DEVICEID Modified by turk
					StringBuilder tmp = new StringBuilder();
					tmp.append("DEVICEID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS',");
					int i = -1;
					Object nullList = new ArrayList<Object>();
					for (strFieldMap = raws.iterator(); ((Iterator<?>)strFieldMap).hasNext(); ) 
					{ 
						String rawName = (String)((Iterator<?>)strFieldMap).next();
						boolean flag = false;
						i++;
						if (colnameToIndex.containsKey(rawName))
						{
							List<?> index = (List<?>)colnameToIndex.get(rawName);
							for (Iterator<?> localIterator2 = index.iterator(); localIterator2.hasNext(); ) 
							{ 
								int ix = ((Integer)localIterator2.next()).intValue();
								if (i != ix)
									continue;
								flag = true;
								break;
							}
						}

						if (flag)
						{
							continue;
						}

						DistributeTemplet.FieldTemplet field = findFieldTemplet(tableInfo.fields, rawName);
						if (field == null)
						{
							if (i <= 2)
								continue;
							((List<Integer>)nullList).add(Integer.valueOf(i));
						}
						else
						{
							switch (field.m_nDataType)
							{
								case 1:
									tmp.append(field.m_strFieldName);
									break;
								case 2:
									tmp.append(field.m_strFieldName + " CHAR(" + 
											field.m_strDataTimeFormat + ")");
									break;
								case 3:
									if (field.m_strFieldName.equals("COLLECTTIME"))
									{
										tmp.append("COLLECT_TIME Date 'YYYY-MM-DD HH24:MI:SS'");
									}
									else
									{
										tmp.append(field.m_strFieldName + 
											" Date 'YYYY-MM-DD HH24:MI:SS'");
									}
									break;
								case 4:
									tmp.append(field.m_strFieldName + 
									" LOBFILE(LOBF_00006) TERMINATED BY EOF ");
									break;
								case 5:
									tmp.append(field.m_strFieldName + 
									" TIMESTAMP '" + field.m_strDataTimeFormat + "'");
									break;
							}

							if (field.m_bIsDefault)
							{
								bw.write(" " + field.m_strDefaultValue);
							}
							if(field.m_DataLength > 0)
							{
								tmp.append(" CHAR(" + field.m_DataLength + ")");
							}
							tmp.append(",");
						}
					}

					if (((List<?>)nullList).size() > 0)
					{
						String txtName = currentPath + File.separatorChar + 
								tempFile + ".txt";
						File txt = new File(txtName);
						InputStream is = new FileInputStream(txt);
						BufferedReader reader = new BufferedReader(new InputStreamReader(is));
						String tmpFile = txtName + ".tmp";
						PrintWriter pw = new PrintWriter(tmpFile);
						String str = null;
						while ((str = reader.readLine()) != null)
						{
							pw.println(str);
							pw.flush();
						}
						pw.close();
						is.close();
						reader.close();
						txt.delete();

						is = new FileInputStream(tmpFile);
						reader = new BufferedReader(new InputStreamReader(is));
						pw = new PrintWriter(txtName);
						while ((str = reader.readLine()) != null)
						{
							String[] items = str.split(splitSign);
							StringBuilder sb = new StringBuilder();
							for (int j = 0; j < items.length; j++)
							{
								boolean flag = false;
								for (Integer index : (List<Integer>)nullList)
								{
									if (j != index.intValue())
										continue;
									flag = true;
								}

								if (flag)
									continue;
								sb.append(items[j]).append(splitSign);
							}

							sb.deleteCharAt(sb.length() - 1);
							pw.println(sb);
							pw.flush();
						}
						pw.close();
						is.close();
						reader.close();
						new File(tmpFile).delete();
					}

					if (tmp.charAt(tmp.length() - 1) == ',')
					{
						tmp.deleteCharAt(tmp.length() - 1);
					}
					bw.write(tmp.toString());
				}
				else
				{
					for (int i = 0; i < tableInfo.fields.size(); i++)
					{
						DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)tableInfo.fields.get(Integer.valueOf(i));
						switch (field.m_nDataType)
						{
							case 1:
								bw.write(field.m_strFieldName);
								break;
							case 2:
								bw.write(field.m_strFieldName + " CHAR(" + 
										field.m_strDataTimeFormat + ")");
								break;
							case 3: //时间格式，精确到秒
								bw.write(field.m_strFieldName + 
								" Date 'YYYY-MM-DD HH24:MI:SS'");
								break;
							case 4:
								bw.write(field.m_strFieldName + 
								" LOBFILE(LOBF_00006) TERMINATED BY EOF ");
								
							case 5://时间格式（精确到毫秒）
								bw.write(field.m_strFieldName + 
										" TIMESTAMP '" + field.m_strDataTimeFormat + "' ");
								break;
						}

						if (field.m_bIsDefault)
						{
							bw.write(" " + field.m_strDefaultValue);
						}
						//Turk Add 字段长度
						if(field.m_DataLength > 0)
						{
							bw.write(" CHAR(" + field.m_DataLength + ")");
						}
						if (i < tableInfo.fields.size() - 1) {
							bw.write(",");
						}
					}
				}

				bw.write(")\r\n");
				bw.close();
        
				Date now = new Date();
				this.collectInfo.setSqlldrTime(new Timestamp(now.getTime()));
				retCode = RunSqlldr(tableIndex, tempFile);
			}
			else if ((Util.isSybase()) || (Util.isSqlServer()))
			{
				Map<?, ?> columns = CommonDB.GetTableColumns(tableInfo.tableName);
				bw.write("10.0\r\n");
				int nField = tableInfo.fields.size();
				bw.write(String.valueOf(nField) + "\r\n");

				for (int i = 1; i <= nField; i++)
				{
					bw.write(i + "\tSYBCHAR\t0\t128\t");
					if (i < nField)
					{
						bw.write("\";\"");
					}
					else
					{
						bw.write("\"\n\"");
					}

					String strField = ((DistributeTemplet.FieldTemplet)tableInfo.fields.get(Integer.valueOf(i - 1))).m_strFieldName;
					int j = 0;
					for (; j < columns.size(); j++)
					{
						if (strField.equalsIgnoreCase((String)columns.get(Integer.valueOf(j))))
						{
							break;
						}
					}
					bw.write("\t" + (j + 1) + "\t" + strField + "\r\n");
				}
				bw.close();
				Date now = new Date();
				this.collectInfo.setSqlldrTime(new Timestamp(now.getTime()));
				RunBcp(tableInfo.tableName, tempFile);
			}
			else if(Util.isMySQL())
			{
				RunMySQLLoadData(tableIndex, tableInfo.tableName, tempFile);
			}
				
			if (SystemConfig.getInstance().isDeleteLog())
			{
				File ctlfile = new File(currentPath, tempFile + ".ctl");
				if (ctlfile.exists()) {
					ctlfile.delete();
				}

				String strTxt = currentPath + File.separatorChar + tempFile + ".txt";
				File txtfile = new File(strTxt);
				if (txtfile.exists())
				{
					if (txtfile.delete())
					{
						logStr = this.collectInfo.getSysName() + ": " + strTxt + "删除成功....";
						this.log.debug(logStr);
						this.collectInfo.log("入库", logStr);
					}
					else
					{
						logStr = this.collectInfo.getSysName() + ": " + strTxt + "删除失败";
						this.log.warn(logStr);
						this.collectInfo.log("入库", logStr);
						
						if(txtfile.getAbsoluteFile().delete())
						{
							logStr = this.collectInfo.getSysName() + ": " + strTxt + "重新删除成功....";
							this.log.debug(logStr);
							this.collectInfo.log("入库", logStr);
						}
						else
						{
							logStr = this.collectInfo.getSysName() + ": " + strTxt + "重新删除失败";
							this.log.warn(logStr);
							this.collectInfo.log("入库", logStr);
						}
					}
				}
				else
				{
					logStr = this.collectInfo.getSysName() + ": " + strTxt + 
						"未找到，无法删除";
					this.log.debug(logStr);
					this.collectInfo.log("入库", logStr);
				}

				if (retCode == 0)
				{
					File txtlog = new File(currentPath + File.separatorChar + 
							"ldrlog", tempFile + ".log");
					if (txtlog.exists())
						txtlog.delete();
				}
			}
		}
		catch (Exception e)
		{
			this.log.error("BuildSqlLdr", e);
			this.collectInfo.log("入库", "BuildSqlLdr", e);
		}
	}

	private boolean isCreateCtlByRawName(Map<Integer, DistributeTemplet.FieldTemplet> fields)
	{
		Collection<DistributeTemplet.FieldTemplet> fs = fields.values();
		for (DistributeTemplet.FieldTemplet f : fs)
		{
			if ((Util.isNull(f.rawName)) && 
					(!f.m_strFieldName.equalsIgnoreCase("omcid")) && 
					(!f.m_strFieldName.equalsIgnoreCase("collecttime")) && 
					(!f.m_strFieldName.equalsIgnoreCase("stamptime"))) 
				return false;
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private DistributeTemplet.FieldTemplet findFieldTemplet(Map<Integer, DistributeTemplet.FieldTemplet> fields, String rawName)
	{
		Map<FieldTemplet, Float> tmp = new HashMap<FieldTemplet, Float>();
		Collection<DistributeTemplet.FieldTemplet> fs = fields.values();
		for (DistributeTemplet.FieldTemplet f : fs)
		{	
			float dis = LevenshteinDistance.similarity(rawName, f.rawName);
			if (dis < SystemConfig.getInstance().getFieldMatch())
				continue;
			tmp.put(f, Float.valueOf(dis));
		}

		Iterator<?> it = tmp.entrySet().iterator();
		Map.Entry max = null;
		while (it.hasNext())
		{
			Map.Entry et = (Map.Entry)it.next();
			if ((max != null) && (((Float)et.getValue()).floatValue() <= ((Float)max.getValue()).floatValue()))
				continue;
			max = et;
		}

		return max == null ? null : (DistributeTemplet.FieldTemplet)max.getKey();
	}

	public String ReadFileFirstLine(String TempFile)
	{
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();

		String strLine = "";
		try
		{
			FileReader reader = new FileReader(strCurrentPath + "/" + TempFile + ".txt");
			BufferedReader br = new BufferedReader(reader);
			strLine = br.readLine();
			br.close();
			reader.close();
		}
		catch (Exception e)
		{
			this.log.error(this.collectInfo.getSysName() + " : ReadFileFirstLine", e);
		}

		return strLine;
	}

	public void RunSqlldr(String strCtlName)
	{
		RunSqlldr(0, strCtlName);
	}

	/**
	 * Oracle 入库方式
	 * @param tableIndex
	 * @param strCtlName
	 * @return
	 */
	public int RunSqlldr(int tableIndex, String strCtlName)
	{
		String logStr = null;
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		
		String strOracleBase = collectInfo.getInDBServerConfig().getInDBServer();
		if(strOracleBase.isEmpty())
		{
			strOracleBase = SystemConfig.getInstance().getDbService();
		}
		
		String strOracleUserName = collectInfo.getInDBServerConfig().getInDBUser();
		if(strOracleUserName.isEmpty())
		{
			strOracleUserName = SystemConfig.getInstance().getDbUserName();
		}
		
		String strOraclePassword = collectInfo.getInDBServerConfig().getInDBPassword();
		if(strOraclePassword.isEmpty())
		{
			strOraclePassword = SystemConfig.getInstance().getDbPassword();
		}
		
		int retCode = -1;
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 " +
				"control=%s%s%s.ctl bad=%s%s%s.bad log=%s%sldrlog%s%s.log " +
				"rows=500 "+ SystemConfig.getInstance().getreadsize() + " errors=999999", 
				new Object[] { strOracleUserName, 
			strOraclePassword, 
			strOracleBase, 
			strCurrentPath, 
			Character.valueOf(File.separatorChar), 
			strCtlName, 
			strCurrentPath, 
			Character.valueOf(File.separatorChar), 
			strCtlName, 
			strCurrentPath, 
			Character.valueOf(File.separatorChar), 
			Character.valueOf(File.separatorChar), 
			strCtlName });
		
		
		try
		{
			
			
			BalySqlloadThread sqlthread = new BalySqlloadThread();
			//sqlthread.setM_TaskInfo(this.collectInfo);
			sqlthread.setTableIndex(tableIndex);

			logStr = this.collectInfo.getSysName() + ": " + 
				cmd.replace(strOracleUserName, "*").replace(strOraclePassword, "*");
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);

			retCode = sqlthread.runcmd(cmd);
			if ((retCode == 0) || (retCode == 2))
			{
				logStr = this.collectInfo.getSysName() + ": sqldr OK. retCode=" + retCode;
				this.log.debug(logStr);
				this.collectInfo.log("入库", logStr);
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

					logStr = this.collectInfo.getSysName() + ": 第" + tryTimes + 
						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					this.log.error(logStr);
					this.collectInfo.log("入库", logStr);
					Thread.currentThread(); Thread.sleep(waitTimeout);
				}

				if ((retCode == 0) || (retCode == 2))
				{
					logStr = this.collectInfo.getSysName() + ": " + tryTimes + 
						"次Sqlldr尝试入库后成功. retCode=" + retCode;
					this.log.info(logStr);
					this.collectInfo.log("入库", logStr);
				}
				else
				{
					logStr = this.collectInfo.getSysName() + " : " + tryTimes + 
						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					this.log.error(logStr);
					this.collectInfo.log("入库", logStr);

					AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 重试" + 
							tryTimes + "次", this.collectInfo.getSysName() + 
							" 返回值=" + retCode, cmd, 30103);
				}
			}
			else
			{
				logStr = this.collectInfo.getSysName() + ": sqlldr 失败 并且不重试.";
				this.log.error(logStr);
				this.collectInfo.log("入库", logStr);
        
				AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 并且不重试", this.collectInfo.getSysName() + 
						" 返回值=" + retCode, cmd, 30101);
			}
		}	
		catch (Exception e)
		{
			logStr = this.collectInfo.getSysName() + ": sqlldr exception. " + cmd;
			this.log.error(logStr, e);
			this.collectInfo.log("入库", logStr, e);

			AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 异常", this.collectInfo.getSysName(), cmd + 
					e.getMessage(), 30102);
		}

		String logFileName = strCurrentPath + File.separator + "ldrlog" + File.separator + strCtlName + ".log";
		File logFile = new File(logFileName);
		if ((!logFile.exists()) || (!logFile.isFile()))
		{
			logStr = this.collectInfo.getSysName() + ": " + logFileName + "不存在.";
			this.log.info(logStr);
			this.collectInfo.log("入库", logStr);
			return retCode;
		}
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try
		{
			SqlldrResult result = analyzer.analysis(new FileInputStream(logFileName));
			if (result == null) {
				return retCode;
			}
			
			//获取入库文件大小
			File file = new File(strCurrentPath+Character.valueOf(File.separatorChar)+strCtlName+".txt");
			double fileBytes = 0;
			if (file.exists()) { 
				FileInputStream fis = null; 
				fis = new FileInputStream(file); 
				fileBytes = fis.available(); 
				fileBytes = (double)fileBytes/(double)1024;
				fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
				fis.close();
			}
			
			logStr = this.collectInfo.getSysName() + ": SQLLDR日志分析结果: DeviceID=" + 
			this.collectInfo.getDevInfo().getDevID() + " 表名=" + 
			result.getTableName() + " 数据时间=" + 
			Util.getDateString(this.collectInfo.getLastCollectTime()) + 
				" 文件大小=" + fileBytes + "KB 文件入库时长=" + result.getRunTime() + "(s) 入库成功条数=" + result.getLoadSuccCount() + " sqlldr日志=" + 
				logFileName;
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);

			dbLogger.log(this.collectInfo.getDevInfo().getDevID(), 
					result.getTableName(), this.collectInfo.getLastCollectTime().getTime(), 
					result.getLoadSuccCount(), this.collectInfo.getTaskID(), 
					result.getRunTime(),fileBytes);
		}
		catch (Exception e)
		{
			logStr = this.collectInfo.getSysName() + ": sqlldr日志分析失败，文件名：" + 
				logFileName + "，原因: ";
			this.log.error(logStr, e);
			this.collectInfo.log("入库", logStr, e);
		}	
		return retCode;
	}

	/**
	 * SQLSERVER OR SYSBASE 入库方式
	 * @param strTable
	 * @param strFormat
	 */
	private void RunBcp(String strTable, String strFormat)
	{
		try
		{
			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strUrl = SystemConfig.getInstance().getDbUrl();
			String strBase = strUrl.substring(strUrl.lastIndexOf("/") + 1);
			String strUserName = SystemConfig.getInstance().getDbUserName();
			String strPassword = SystemConfig.getInstance().getDbPassword();
			String strService = SystemConfig.getInstance().getDbService();
			String strLog = strCurrentPath + File.separatorChar + "ldrlog" + 
				File.separatorChar + strFormat + ".log";
			String strDataFile = strCurrentPath + File.separatorChar + 
				strFormat + ".txt";

			String cmd = String.format("bcp %s..%s in \"%s\" -U%s -P%s -S%s -t; -r\\n -c -e %s", new Object[] { strBase, strTable, strDataFile, strUserName, strPassword, strService, strLog });

			Process ldr = Runtime.getRuntime().exec(cmd);
				ldr.waitFor();
		}
		catch (Exception e)
		{
			this.log.error("BCP Error!", e);
		}
	}
	
	
	/**
	 * MySql 入库方式
	 * @param strTable
	 * @param strFormat
	 */
	private void RunMySQLLoadData(int tableIndex,String strTable, String strFormat)
	{
		try
		{
			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();

			String strHost = SystemConfig.getInstance().getDbService();
			String strInDB = collectInfo.getInDBServerConfig().getInDBServer();
			if(strInDB.isEmpty())
			{
				strInDB = SystemConfig.getInstance().getDbService();
			}
			
			String strInDBUser = collectInfo.getInDBServerConfig().getInDBUser();
			if(strInDBUser.isEmpty())
			{
				strInDBUser = SystemConfig.getInstance().getDbUserName();
			}
			
			String strInDBPassword = collectInfo.getInDBServerConfig().getInDBPassword();
			if(strInDBPassword.isEmpty())
			{
				strInDBPassword = SystemConfig.getInstance().getDbPassword();
			}

			String logStr = "";
			
			BalySqlloadThread sqlthread = new BalySqlloadThread();
			sqlthread.setTableIndex(tableIndex);

			String sourceFile = strCurrentPath + File.separatorChar + 
			strFormat + ".txt";
			String targetFile = strCurrentPath + File.separatorChar + strTable + ".txt";
			Util.FileCopy(sourceFile, targetFile);
			
			String cmd = String.format("mysqlimport -h %s -L -u %s -p%s --fields-terminated-by=; --ignore-lines=1 %s %s", 
					new Object[] { strHost, strInDBUser, strInDBPassword, strInDB, targetFile });
			logStr = this.collectInfo.getSysName() + ": " + 
				cmd.replace(strInDBUser, "*").replace(strInDBPassword, "*");
			
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);
		
			int retCode = sqlthread.runcmd(cmd);
			
			File delfile = new File(sourceFile);
			if(delfile.delete())
			{
				this.log.debug("删除文件:"+sourceFile+" 成功");
			}
			
			if ((retCode == 0) || (retCode == 2))
			{
				logStr = this.collectInfo.getSysName() + ": sqldr OK. retCode=" + retCode;
				this.log.debug(logStr);
				this.collectInfo.log("入库", logStr);
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

					logStr = this.collectInfo.getSysName() + ": 第" + tryTimes + 
						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					this.log.error(logStr);
					this.collectInfo.log("入库", logStr);
					Thread.currentThread(); Thread.sleep(waitTimeout);
				}

				if ((retCode == 0) || (retCode == 2))
				{
					logStr = this.collectInfo.getSysName() + ": " + tryTimes + 
						"次Sqlldr尝试入库后成功. retCode=" + retCode;
					this.log.info(logStr);
					this.collectInfo.log("入库", logStr);
				}
				else
				{
					logStr = this.collectInfo.getSysName() + " : " + tryTimes + 
						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					this.log.error(logStr);
					this.collectInfo.log("入库", logStr);

					AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 重试" + 
							tryTimes + "次", this.collectInfo.getSysName() + 
							" 返回值=" + retCode, cmd, 30103);
				}
			}
			else
			{
				logStr = this.collectInfo.getSysName() + ": sqlldr 失败 并且不重试.";
				this.log.error(logStr);
				this.collectInfo.log("入库", logStr);
        
				AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 并且不重试", this.collectInfo.getSysName() + 
						" 返回值=" + retCode, cmd, 30101);
			}
			
			
			//获取入库文件大小
			File file = new File(targetFile);
			double fileBytes = 0;
			if (file.exists()) { 
				FileInputStream fis = null; 
				fis = new FileInputStream(file); 
				fileBytes = fis.available(); 
				fileBytes = (double)fileBytes/(double)1024;
				fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
				fis.close();
			}
			
			logStr = this.collectInfo.getSysName() + ": SQLLDR日志分析结果: DeviceID=" + 
			this.collectInfo.getDevInfo().getDevID() + " 表名=" + 
			strTable.toUpperCase() + " 数据时间=" + 
			Util.getDateString(this.collectInfo.getLastCollectTime()) + 
				" 文件大小=" + fileBytes + "KB 入库成功条数=" + sqlthread.getRecordNum();
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);

			dbLogger.log(this.collectInfo.getDevInfo().getDevID(), 
					strTable.toUpperCase(), this.collectInfo.getLastCollectTime().getTime(), 
					sqlthread.getRecordNum(), this.collectInfo.getTaskID(), 
					0,fileBytes);
			
		}
		catch (Exception e)
		{
			this.log.error("MySQLLoadData Error!", e);
		}
	}
	
}