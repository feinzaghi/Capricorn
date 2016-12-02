package com.turk.parser.taurus;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.turk.db.GPCommonDB;
import com.turk.util.Import;

public class ImportData extends Import{

	public ImportData()
	{
		super(";");
	}
	
	public ImportData(String strSplit){
		super(strSplit);
	}
	
	/**
	 * 设置是否立即入库
	 * @param execimmediate
	 */
	public ImportData(boolean execimmediate){
		super(execimmediate);
	}
	
	
	public boolean ImportMonitorHit(int monitor_id,
			String user_name,
				String monitor_name,
			  String case_id,
			  String hit_item,
			  Date hit_time,
			  String cell_name,
			  long cell_sys_id,
			  String action,
			  String alarm_level,
			  String msisdn,
			  String confirm,
			  Date confirm_time,
			  String confirm_name)
	{
		String strLine = "";
		try
		{
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			strLine = String.format("%d,'%s','%s','%s','%s','%s','%s',%d,'%s','%s','%s',null,null,'%s'",
					monitor_id,user_name,
					monitor_name,case_id,hit_item,formatter.format(hit_time),
					cell_name,cell_sys_id,action,alarm_level,msisdn,
					confirm_name);
			
			String strSql = String.format("INSERT INTO mod_monitor_hit VALUES(%s)", strLine);
			GPCommonDB.executeUpdate(strSql);
			//WriteFileLine(strLine,"UTF-8");
			
			return true;
		}
		catch(Exception ex)
		{
			log.error("WRITE FILE:" + strLine,ex);
			return false;
		}
	}

}
