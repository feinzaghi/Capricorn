package com.turk.access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import com.turk.alarm.AlarmMgr;
import com.turk.config.ConstDef;
import com.turk.config.SystemConfig;
import com.turk.distributor.DistributeTemplet;
import com.turk.parser.LineParser;
import com.turk.task.RegatherObjInfo;
import com.turk.task.TaskMgr;
import com.turk.task.TaskMgr.RedoSQL;
import com.turk.util.CommonDB;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * 数据库采集访问
 * @author Administrator
 *
 */
public class DBAccessor extends AbstractDBAccessor
{
	public boolean access()
    	throws Exception
    {
		boolean bSucceed = false;

		DistributeTemplet templetD = (DistributeTemplet)this.taskInfo.getDistributeTemplet();
		int maxRecltTime = this.taskInfo.getMaxReCollectTime();
		String currentTableName = null;

		String logStr = null;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsm = null;
		List<RedoSQL> redoSqlList = null;

		int sqlIndex = 0;

		boolean exception = false;
		String exceptionMsg = null;

		boolean isRedoFlag = false;
		int sqlLen;
		String redoSql;
    
		try
    	{
			conn = getConnection();

			String[] strNeedGatherFileNames = getDataSourceConfig().getDatas();

			execShellBeforeAccess();
			isRedoFlag = TaskMgr.getInstance().isReAdoptObj(this.taskInfo);
			redoSqlList = new ArrayList<RedoSQL>();
			for (int k = 0; k < strNeedGatherFileNames.length; k++)
			{
				sqlIndex = k;
				if (Util.isNull(strNeedGatherFileNames[k])) {
					continue;
				}
				currentTableName = ((DistributeTemplet.TableTemplet)templetD.tableTemplets.get(Integer.valueOf(k))).tableName;
				this.parser.setFileName("TABLE_" + k);
				String strNewSQL = null;
				strNewSQL = ConstDef.ParseFilePathForDB(strNeedGatherFileNames[k].trim(), this.taskInfo.getLastCollectTime());

				String tableName = CommonDB.getTableName(strNewSQL);

				int tempIndex = strNewSQL.indexOf("@");
				int redoTableIndex = -1;
				if (isRedoFlag)
				{
					if (tempIndex != -1)
					{
						redoTableIndex = Integer.parseInt(strNewSQL.substring(tempIndex + 1).trim());
						strNewSQL = strNewSQL.substring(0, tempIndex);

						((RegatherObjInfo)this.taskInfo).addTableIndex(redoTableIndex);

						this.parser.setFileName("TABLE_" + redoTableIndex);
						logStr = this.name + " : reAdopt table index ： TABLE_" + 
							redoTableIndex;
						this.log.debug(logStr);
						this.taskInfo.log("开始", logStr);
					}
					else
					{
						((RegatherObjInfo)this.taskInfo).addTableIndex(k);
					}

				}

				boolean errorFlag = false;
				String sqlEx = null;
				try
				{
					stmt = conn.prepareStatement(strNewSQL);
					stmt.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
					rs = stmt.executeQuery();
					rsm = rs.getMetaData();
				}
				catch (Exception e)
				{
					if ((maxRecltTime <= -2) && 
							(!CommonDB.tableExists(conn, tableName, this.taskInfo.getTaskID())))
					{
						logStr = "表" + tableName + "不存在，且MAXCLTTIME<=-2，忽略此表";
						this.log.warn(logStr);
						this.taskInfo.log("开始", logStr);
					}
					else
					{
						errorFlag = true;
						sqlEx = e.getMessage() == null ? "" : e.getMessage();
						logStr = this.name + ": SQL执行失败:" + strNewSQL + " Cause: ";
						this.log.error(logStr, e);
						this.taskInfo.log("开始", logStr, e);

						AlarmMgr.getInstance().insert(this.taskInfo.getTaskID(),(byte)1, "SQL执行失败", this.name, strNewSQL + 
								" " + e.getMessage(), 10202);
					}
				}

				if (errorFlag)
				{
					redoSql = null;

					if ((tempIndex == -1) && (!isRedoFlag))
					{
						redoSql = strNewSQL + "@" + k;
						logStr = this.name + ": add reAdapt SQL :" + strNewSQL + "@" + 
							k;
						this.log.debug(logStr);
						this.taskInfo.log("开始", logStr);
					}
					else
					{
						redoSql = strNewSQL + "@" + redoTableIndex;
						logStr = this.name + ": add reAdapt SQL :" + strNewSQL + "@" + 
							redoTableIndex;
						this.log.debug(logStr);
						this.taskInfo.log("开始", logStr);
					}
					if (redoSql != null)
					{
						redoSqlList.add(
								new TaskMgr.RedoSQL(redoSql, "执行select语句时异常，异常信息为：" + 
										sqlEx));
					}
					this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(), currentTableName, this.taskInfo.getLastCollectTime(), -1, 
							this.taskInfo.getTaskID(),0);
				}
				else
				{
					logStr = this.name + ": SQL执行成功: " + strNewSQL;
					this.log.debug(logStr);
					this.taskInfo.log("开始", logStr);

					StringBuffer buf = new StringBuffer();
					int nLineIndex = 0;
					long recordCount = 0L;
					try
					{
						while (rs.next())
						{
							int nColumnCount = rsm.getColumnCount();
							for (int ii = 0; ii < nColumnCount; ii++)
							{
								String strValue = rs.getString(ii + 1);

								if ((rsm.getColumnType(ii + 1) == 91) || 
										(rsm.getColumnType(ii + 1) == 92) || 
										(rsm.getColumnType(ii + 1) == 93))
								{
									if (strValue == null)
										strValue = "";
									else
										strValue = strValue.substring(0, 19);
								}
								if (strValue == null) {
									strValue = "";
								}

								strValue = removeNoise(strValue);
								
								if (ii < nColumnCount - 1)
									buf.append(strValue + ";");
								else
									buf.append(strValue + ";0\n");
							}
							nLineIndex++;
							recordCount += 1L;
              
							if (nLineIndex % 1000 != 0) {
								continue;
							}
							((LineParser)this.parser).BuildData(buf.toString().toCharArray(), buf.length());
								buf.delete(0, buf.length());
								nLineIndex = 0;
						}

						buf.append("**FILEEND**\n");
						((LineParser)this.parser).BuildData(buf.toString().toCharArray(), buf.length());

						logStr = this.name + ": " + strNewSQL + " 数据采集完成.此SQL语句共采集数据:" + 
							recordCount;
						this.log.debug(logStr);
						this.taskInfo.log("开始", logStr);
					}
					catch (Exception e)
					{
						errorFlag = true;
						sqlEx = e.getMessage() == null ? "" : e.getMessage();
						logStr = this.name + ": 数据库采集失败.ResultSet获取数据异常! Cause: ";
						this.log.error(logStr, e);
						this.taskInfo.log("开始", logStr, e);

						AlarmMgr.getInstance().insert(this.taskInfo.getTaskID(),(byte)2, "ResultSet获取数据异常!", this.name, strNewSQL + 
								" " + e.getMessage(), 10203);
					}
					if (errorFlag)
					{
						redoSql = null;
						
						if ((tempIndex == -1) && (!isRedoFlag))
						{
							redoSql = strNewSQL + "@" + k;
							logStr = this.name + ": add reAdapt SQL :" + strNewSQL + "@" + 
								k;
							this.log.debug(logStr);
							this.taskInfo.log("开始", logStr);
						}
						else
						{
							redoSql = strNewSQL + "@" + redoTableIndex;
							logStr = this.name + ": add reAdapt SQL :" + strNewSQL + "@" + 
								redoTableIndex;
							this.log.debug(logStr);
							this.taskInfo.log("开始", logStr);
						}
						if (redoSql != null)
						{
							redoSqlList.add(
									new TaskMgr.RedoSQL(redoSql, "从ResultSet（select出的结果集）中获取数据时异常，异常信息为：" + 
											sqlEx));
						}
						this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(), currentTableName, 
								this.taskInfo.getLastCollectTime(), -1, this.taskInfo.getTaskID(),0);
					}
					else
					{
						if ((recordCount != 0L) || (maxRecltTime <= -1))
							continue;
						redoSql = null;

						if (tempIndex == -1)
						{
							redoSql = strNewSQL + "@" + k;
							logStr = this.name + ": add reAdapt SQL :" + strNewSQL + "@" + k;
							this.log.debug(logStr);
							this.taskInfo.log("开始", logStr);
						}
						else
						{
							redoSql = strNewSQL + "@" + redoTableIndex;
							logStr = this.name + ": add reAdapt SQL :" + strNewSQL + "@" + 
								redoTableIndex;
							this.log.debug(logStr);
							this.taskInfo.log("开始", logStr);
						}
						if (redoSql != null)
						{
							redoSqlList.add(new TaskMgr.RedoSQL(redoSql, "select出来的记录数为0"));
						}
						this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(), 
								currentTableName, this.taskInfo.getLastCollectTime(), -1, 
								this.taskInfo.getTaskID(),0);
					}
				}
			}

			bSucceed = true;
    	}
		catch (Exception e)
		{
			logStr = this.name + ": 数据库采集失败. Cause: ";
			this.log.error(logStr, e);
			this.taskInfo.log("开始", logStr, e);
			bSucceed = false;
			exception = true;
			exceptionMsg = e.getMessage() == null ? "" : e.getMessage();

			AlarmMgr.getInstance().insert(this.taskInfo.getTaskID(),(byte)2, "数据库采集失败", this.name, "原因:" + 
					e.getMessage(), 10204);

			if (exception)
			{
				sqlLen = getDataSourceConfig().getDatas().length;
				if (sqlIndex < sqlLen)
				{
					for (int ii = sqlIndex; ii < sqlLen; ii++)
					{
						redoSql = null;

						if (isRedoFlag)
						{
							redoSql = getDataSourceConfig().getDatas()[ii];
						}
						else
						{
							redoSql = getDataSourceConfig().getDatas()[ii] + "@" + ii;
						}
						redoSqlList.add(
								new TaskMgr.RedoSQL(redoSql, "数据库采集时发生异常，异常信息:" + 
										exceptionMsg));
					}
				}
			}

			if (redoSqlList.size() > 0)
			{
				StringBuilder sb = new StringBuilder();
				StringBuilder cause = new StringBuilder();
				String sql = null;
				for (int i = 0; i < redoSqlList.size() - 1; i++)
				{
					sql = ((TaskMgr.RedoSQL)redoSqlList.get(i)).sql;
					if (sql == null)
						continue;
					sb.append(sql + ";");
					cause.append("语句\"").append(sql).append("\"补采原因为:").append(((TaskMgr.RedoSQL)redoSqlList.get(i)).cause).append("\n\n");
				}
				sb.append(((TaskMgr.RedoSQL)redoSqlList.get(redoSqlList.size() - 1)).sql);
				cause.append("语句\"").append(((TaskMgr.RedoSQL)redoSqlList.get(redoSqlList.size() - 1)).sql).append("\"补采原因为:").append(((TaskMgr.RedoSQL)redoSqlList.get(redoSqlList.size() - 1)).cause).append("\n\n");
				logStr = this.name + " add  reAdapt filepath ,the SQL is :" + sb.toString();
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);

				TaskMgr.getInstance().newRegather(this.taskInfo, sb.toString(), cause.toString());
				sb.delete(0, sb.length());
				redoSqlList.clear();
			}
			try
			{
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			}
			catch (Exception localException1)
			{
			}
		}
		finally
		{
			if (exception)
			{
				sqlLen = getDataSourceConfig().getDatas().length;
				if (sqlIndex < sqlLen)
				{
					for (int i = sqlIndex; i < sqlLen; i++)
					{
						redoSql = null;
            
						if (isRedoFlag)
						{
							redoSql = getDataSourceConfig().getDatas()[i];
						}
						else
						{
							redoSql = getDataSourceConfig().getDatas()[i] + "@" + i;
						}
						redoSqlList.add(
								new TaskMgr.RedoSQL(redoSql, "数据库采集时发生异常，异常信息:" + 
										exceptionMsg));
					}
				}
			}

			if (redoSqlList.size() > 0)
			{
				StringBuilder sb = new StringBuilder();
				StringBuilder cause = new StringBuilder();
				String sql = null;
				for (int i = 0; i < redoSqlList.size() - 1; i++)
				{
					sql = ((TaskMgr.RedoSQL)redoSqlList.get(i)).sql;
					if (sql == null)
						continue;
					sb.append(sql + ";");
					cause.append("语句\"").append(sql).append("\"补采原因为:").append(((TaskMgr.RedoSQL)redoSqlList.get(i)).cause).append("\n\n");
				}
				sb.append(((TaskMgr.RedoSQL)redoSqlList.get(redoSqlList.size() - 1)).sql);
				cause.append("语句\"").append(((TaskMgr.RedoSQL)redoSqlList.get(redoSqlList.size() - 1)).sql).append("\"补采原因为:").append(((TaskMgr.RedoSQL)redoSqlList.get(redoSqlList.size() - 1)).cause).append("\n\n");
				logStr = this.name + " add  reAdapt filepath ,the SQL is :" +  sb.toString();
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);

				TaskMgr.getInstance().newRegather(this.taskInfo, sb.toString(), cause.toString());
				sb.delete(0, sb.length());
				redoSqlList.clear();
			}
			try
			{
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null) {
					conn.close();
				}
			}
			catch (Exception localException2)
			{
			}
		}
		return bSucceed;
    }

	private String removeNoise(String content)
	{
		String strValue = content.replaceAll(";", " ");

		strValue = strValue.replaceAll("\r\n", " ");
		strValue = strValue.replaceAll("\n", " ");
		strValue = strValue.replaceAll("\r", " ");
		strValue = strValue.trim();

		return strValue;
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Task taskCore() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean useDb() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stopTask() {
		// TODO Auto-generated method stub
		
	}
}