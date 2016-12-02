package com.turk.access;

import com.turk.Config.ConstDef;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.turk.alarm.AlarmMgr;
import com.turk.task.TaskMgr;
import com.turk.util.CommonDB;
import com.turk.util.DBLogger;
import com.turk.util.LogMgr;
import com.turk.util.Parsecmd;
import com.turk.util.Util;

/**
 * ���ݿ���ʳ�����
 * @author Administrator
 *
 */
public abstract class AbstractDBAccessor extends AbstractAccessor
{
	/**
	 * ����Դ���
	 */
	protected static final byte MAX_TRY_TIMES = 5;
	/**
	 * ����ʱ��
	 */
	protected static final int SLEEP_TIME = 5000;
	protected List<TaskMgr.RedoSQL> redoSqlList = null;

	protected DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public abstract boolean access()
		throws Exception;

	public void configure()
    	throws Exception
    {
    }

	public boolean doAfterAccess()
    	throws Exception
    {
		String cmd = this.taskInfo.getShellCmdFinish();
		if (Util.isNotNull(cmd))
		{
			Parsecmd.ExecShellCmdByFtp(cmd, this.taskInfo.getLastCollectTime());
		}

		return true;
    }

	protected Connection getConnection()
     	throws Exception
    {
		Connection conn = TaskMgr.getInstance().getConnection(this.taskInfo, 30000, (byte)5);
		if (conn == null)
		{
			AlarmMgr.getInstance().insert(this.taskInfo.getTaskID(),(byte)2, "��λ�ȡ���ݿ�����ʧ��", this.taskInfo.getDBUrl(), this.name, 10201);
			throw new Exception("��λ�ȡ���ݿ�����ʧ��");
		}

		return conn;
    }

	/**
	 * �ɼ�ǰִ��Shell����
	 * @throws Exception
	 */
	protected void execShellBeforeAccess()
    	throws Exception
    {
		String preCmd = this.taskInfo.getShellCmdPrepare();
		if (Util.isNotNull(preCmd))
		{
			boolean b = execSql(preCmd);
			if (!b)
				throw new Exception("����(" + preCmd + ")ִ��ʧ��");
		}
    }

	private boolean execSql(String strSQLList)
	{
		PreparedStatement pstmt = null;
		Connection conn = null;
		boolean bSuccesed = true;
		try
		{
			conn = CommonDB.getConnection();
			String[] strSQL = strSQLList.split(";");
			for (int i = 0; i < strSQL.length; i++)
			{
				if ((strSQL[i] == null) || (strSQL[i].equals("")))
					continue;
				String strSql = ConstDef.ParseFilePath(strSQL[i], this.taskInfo.getLastCollectTime());
				synchronized (conn)
				{
					pstmt = conn.prepareStatement(strSql);
					pstmt.execute(strSQL[i]);
				}
			}

		}
		catch (Exception e)
		{
			String logStr = this.name + ": ִ��SQL���ʧ��,ԭ��:";
			this.log.error(logStr, e);
			this.taskInfo.log("��ʼ", logStr);
			bSuccesed = false;
		}
		finally
		{
			CommonDB.close(null, pstmt, conn);
		}

		return bSuccesed;
	}

	/**
	 * �жϱ��Ƿ����
	 * @param conn
	 * @param sqlTName
	 * @return
	 */
	protected boolean tablesExists(Connection conn, String sqlTName)
	{
		boolean flag = true;
		String logStr = null;
		if ((sqlTName == null) || (sqlTName.equals(""))) 
			return flag;

		try
		{
			if (!CommonDB.tableExists(conn, sqlTName, this.taskInfo.getTaskID()))
			{
				logStr = this.taskInfo.getTaskID() + ": DeviceID =" + 
					this.taskInfo.getDevInfo().getDevID() + " �����˱�Ĳɼ�,ԭ��:��(" + 
					sqlTName + ")������" + " ����ʱ�䣺 " + 
					this.taskInfo.getLastCollectTime();

				this.log.error(logStr);
				this.taskInfo.log("��ʼ", logStr);
				flag = false;
			}
		}
		catch (SQLException e)
		{
			logStr = this.taskInfo.getTaskID() + ": ����(" + sqlTName + ")�Ƿ����ʱ�쳣," + 
				" ����ʱ�䣺 " + this.taskInfo.getLastCollectTime() + ", ԭ��:";
			this.log.error(logStr, e);
			this.taskInfo.log("��ʼ", logStr, e);
		}
		return flag;
	}

	protected String toSql(String tableName, String condition)
	{
		String sql = null;
		if (Util.isNotNull(tableName))
		{
			sql = "select * from " + tableName;
			if (Util.isNotNull(condition))
			{
				condition = ConstDef.ParseFilePathForDB(condition, this.taskInfo.getLastCollectTime());
				sql = sql + " where " + condition;
			}
		}
		return sql;
	}

	protected void commitFailSql()
	{
		if (this.redoSqlList == null)
		{
			TaskMgr.getInstance().newRegather(this.taskInfo, "", "���ݿ�ɼ�ʧ��(��δִ��select���)�� �������б�");
			return;
		}

		if (this.redoSqlList.size() > 0)
		{
			StringBuilder sb = new StringBuilder();
			StringBuilder cause = new StringBuilder();
			String sql = null;
			for (int i = 0; i < this.redoSqlList.size() - 1; i++)
			{
				sql = ((TaskMgr.RedoSQL)this.redoSqlList.get(i)).sql;
				if (Util.isNull(sql))
					continue;
				sb.append(sql + ";");
				cause.append("\"").append(sql).append("\"����ԭ��Ϊ:").append(((TaskMgr.RedoSQL)this.redoSqlList.get(i)).cause).append("\n\n");
			}
			sb.append(((TaskMgr.RedoSQL)this.redoSqlList.get(this.redoSqlList.size() - 1)).sql);
			cause.append("\"").append(((TaskMgr.RedoSQL)this.redoSqlList.get(this.redoSqlList.size() - 1)).sql).append("\"����ԭ��Ϊ:").append(((TaskMgr.RedoSQL)this.redoSqlList.get(this.redoSqlList.size() - 1)).cause).append("\n\n");
			String str = sb.toString();
			String logStr = this.name + " fail table-name:" + str;
			this.log.info(logStr);
			this.taskInfo.log("��ʼ", logStr);
			
			TaskMgr.getInstance().newRegather(this.taskInfo, str, cause.toString());
			sb.delete(0, sb.length());
			this.redoSqlList.clear();
			this.redoSqlList = null;
		}
	}
}