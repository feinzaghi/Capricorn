package com.turk.access;

import com.turk.Config.ConstDef;
import com.turk.Config.SystemConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.turk.alarm.AlarmMgr;
import com.turk.parser.DBAutoParser;
import com.turk.task.IgnoresInfo;
import com.turk.task.IgnoresMgr;
import com.turk.task.TaskMgr;
import com.turk.task.TaskMgr.RedoSQL;
import com.turk.templet.DBAutoTempletP;
import com.turk.templet.GenericSectionHeadD;
import com.turk.templet.Table;
import com.turk.templet.TempletBase;
import com.turk.templet.DBAutoTempletP.Templet;
import com.turk.util.CommonDB;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * 数据库自动采集类
 * @author Administrator
 *
 */
public class DBAutoAccessor extends AbstractDBAccessor
{
	private Collection<DBAutoTempletP.Templet> templets = null;

	private IgnoresMgr ignoresMgr = IgnoresMgr.getInstance();

	public boolean validate()
	{
		String logStr = null;

		if (this.taskInfo == null) {
			return false;
		}

		try
		{
			this.strLastGatherTime = Util.getDateString(this.taskInfo.getLastCollectTime());
		}
		catch (Exception e)
		{
			logStr = this.name + "> 时间格式错误,原因:";
			this.log.error(logStr, e);
			this.taskInfo.log("开始", logStr, e);
			return false;
		}

		TempletBase tBase = this.taskInfo.getParseTemplet();
		if (!(tBase instanceof DBAutoTempletP)) {
			return false;
		}

		return (this.parser instanceof DBAutoParser);
	}

	public boolean access()
		throws Exception
	{
		String logStr = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		int maxRecltTime = this.taskInfo.getMaxReCollectTime();
		try
		{
			conn = getConnection();

			execShellBeforeAccess();

			this.redoSqlList = new ArrayList<RedoSQL>();
			DBAutoParser myParser = (DBAutoParser)this.parser;
			Map<String, Templet> templetsP = ((DBAutoTempletP)this.taskInfo.getParseTemplet()).getTemplets();

			setTemplets(templetsP);

			for (DBAutoTempletP.Templet t : this.templets)
			{
				GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD)this.taskInfo.getDistributeTemplet()).getTemplet(t.getId());

				if ((t == null) || (!t.isUsed())) {
					continue;
				}
				Timestamp lastCollectTime = this.taskInfo.getLastCollectTime();

				String tableName = t.getTableName();

				String sqlTName = ConstDef.ParseFilePathForDB(tableName, lastCollectTime);

				boolean flag = tablesExists(conn, sqlTName);
				if (!flag)
				{
					this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(), 
							((Table)templetD.getTables().get(Integer.valueOf(0))).getName(), 
							this.taskInfo.getLastCollectTime(), -1, this.taskInfo.getTaskID(),0);
					if (!t.isOccur())
						continue;
					IgnoresInfo ignoresInfo = this.ignoresMgr.checkIgnore(this.taskInfo.getTaskID(), sqlTName, this.taskInfo.getLastCollectTime());
					if (ignoresInfo == null)
					{
						this.redoSqlList.add(
								new TaskMgr.RedoSQL(tableName, " 表(" + 
										tableName + ")不存在."));
					}
					else
					{
						this.log.warn(this.name + " " + sqlTName + 
								"不存在,但igp_conf_ignores表中设置了忽略此路径(" + 
								ignoresInfo + "),不加入补采表.");
					}
				}
				else
				{
					IgnoresInfo ignoresInfo = this.ignoresMgr.checkIgnore(this.taskInfo.getTaskID(), sqlTName, this.taskInfo.getLastCollectTime());
					if (ignoresInfo != null)
					{
						this.log.warn(this.name + " " + sqlTName + 
								",igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + 
						"),  但本次发现其存在,以后将不再忽略此路径.");
						ignoresInfo.setNotUsed();
					}

					String sql = getSql(lastCollectTime, t, sqlTName);

					boolean exceptionFlag = false;

					int recordCount = 0;
					try
					{
						ps = conn.prepareStatement(sql);
						ps.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
						rs = ps.executeQuery();

						recordCount = myParser.parseData(rs, t);
						if ((recordCount == 0) && (maxRecltTime > -1))
						{
							this.redoSqlList.add(new TaskMgr.RedoSQL(tableName, "select出的记录数为0"));
							this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(), 
									((Table)templetD.getTables().get(Integer.valueOf(0))).getName(), 
									this.taskInfo.getLastCollectTime(), 0, this.taskInfo.getTaskID(),0);
						}
					}
					catch (Exception e)
					{
						exceptionFlag = true;
					}

					if (exceptionFlag) {
						recordCount = reCollect(myParser, t, templetD, conn, sql, tableName, maxRecltTime);
					}
					if (recordCount == -1)
						continue;
					logStr = this.name + ": SQL采集成功，数量= " + recordCount + " SQL=" + sql;
					this.log.debug(logStr);
					this.taskInfo.log("开始", logStr);
				}
			}
		}
		finally {
			commitFailSql();
			CommonDB.close(rs, ps, conn);
		}

		return true;
	}

	private String getSql(Timestamp lastCollectTime, DBAutoTempletP.Templet t, String sqlTName)
	{
		String sql = t.getSql();

		if ((sql != null) && (!sql.equals("")))
		{
			sql = ConstDef.ParseFilePathForDB(sql, lastCollectTime);
		}
		else
		{
			String condition = t.getCondition();
			sql = toSql(sqlTName, condition);
		}
		return sql;
	}

	private int reCollect(DBAutoParser myParser, DBAutoTempletP.Templet t, GenericSectionHeadD.Templet templetD, Connection conn, String sql, String tableName, int maxRecltTime)
	{
		int recordCount = -1;
		PreparedStatement ps = null;
		ResultSet rs = null;

		int currentTry = 1;
		String logStr = null;
		while (currentTry <= 5)
		{
			try
			{
				Thread.sleep(5000L);
				logStr = this.name + ":开始第" + currentTry + "次从数据库重试进行采集!";
				this.log.info(logStr);
				conn = getConnection();

				execShellBeforeAccess();
				ps = conn.prepareStatement(sql);
				ps.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
				rs = ps.executeQuery();

				recordCount = myParser.parseData(rs, t);
				if ((recordCount == 0) && (maxRecltTime > -1))
				{
					this.redoSqlList.add(new TaskMgr.RedoSQL(tableName, "select出的记录数为0"));
					this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(), 
							((Table)templetD.getTables().get(Integer.valueOf(0))).getName(), 
							this.taskInfo.getLastCollectTime(), 0, this.taskInfo.getTaskID(),0);
				}
				logStr = this.name + ":第" + currentTry + "次从数据库重试成功!";
				this.log.info(logStr);

				CommonDB.close(rs, ps, conn);
			}
			catch (Exception e)
			{
				if (currentTry == 5)
				{
					this.redoSqlList.add(
							new TaskMgr.RedoSQL(tableName, "执行select时异常，异常信息为:" + 
									e.getMessage()));
					logStr = this.name + ": SQL执行失败,并进行了" + currentTry + "次重试,sql=(" + 
						sql + "),原因: ";
					this.log.error(logStr, e);
					this.taskInfo.log("开始", logStr, e);

					AlarmMgr.getInstance().insert(getTaskID(),(byte)1, "SQL执行失败", this.name, sql + 
							" " + e.getMessage(), 10205);
					this.dbLogger.log(this.taskInfo.getDevInfo().getDevID(),
							((Table)templetD.getTables().get(Integer.valueOf(0))).getName(),
							this.taskInfo.getLastCollectTime(), 0, this.taskInfo.getTaskID(),0);
				}

				CommonDB.close(rs, ps, conn); } finally { CommonDB.close(rs, ps, conn);
			}
		}

		return recordCount;
	}

	private void setTemplets(Map<String, DBAutoTempletP.Templet> templetsP)
	{
		this.templets = new ArrayList<Templet>();
		boolean isRegatherObj = TaskMgr.getInstance().isReAdoptObj(this.taskInfo);
		if (isRegatherObj)
		{
			String[] paths = getDataSourceConfig() != null ? getDataSourceConfig().getDatas() : null;
			
			if ((paths == null) || (paths.length == 0))
			{
				this.templets = templetsP.values();
			}
			else
			{
				List<Templet> list = new ArrayList<Templet>();
				for (String tableName : paths)
				{
					if (Util.isNull(tableName))
						continue;
					list.add(templetsP.get(tableName));
				}
				this.templets.addAll(list);
			}
		}
		else
		{
			this.templets = templetsP.values();
		}
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