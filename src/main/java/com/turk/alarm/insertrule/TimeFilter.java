package com.turk.alarm.insertrule;

import java.text.SimpleDateFormat;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.log4j.Logger;

import com.turk.alarm.Alarm;
import com.turk.alarm.RuleFilter;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;

public class TimeFilter
  implements RuleFilter
{
	private String timeSql = "SELECT * FROM UTL_DATA_ALARM WHERE OCCUREDTIME>to_date('%s','YYYY-MM-DD HH24:MI:SS') AND TASKID=%s";
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public boolean doFilter(Alarm alarm)
	{
		boolean flag = true;

		String sql = String.format(this.timeSql, new Object[] { new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.valueOf(alarm.getOccuredTime().getTime() - 600000L)), Integer.valueOf(alarm.getTaskID()) });
		Result result = null;
		try
		{
			result = CommonDB.queryForResult(sql);
		}
		catch (Exception e)
		{
			log.error("时间过滤时发生错误！", e);
		}

		if ((result != null) && (result.getRowCount() > 0))
		{
			flag = false;
		}
		return flag;
	}
}