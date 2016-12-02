package com.turk.collect;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.turk.parser.Parser;
import com.turk.task.CollectObjInfo;
import com.turk.templet.hw.M2000AlarmTempletP;
import com.turk.util.DbPool;

public class SocketCollect extends Parser
{
	static Socket server;
	private byte[] buffer;
	private int readCount = 0;
	private List<String> sqlList = null;

	private String defaultDate = "1970-01-01 08:00:00";
	char[] head;
	char[] tail;
	char[] middle;
	char[] nextHead;
	char[] nextTail;
	private CollectObjInfo collectObjInfo = null;

	public SocketCollect()
	{
	}

	public SocketCollect(CollectObjInfo collectObjInfo)
	{
		this.collectObjInfo = collectObjInfo;
	}

	public void start()
	{
		this.buffer = new byte[8192];
		this.sqlList = new ArrayList<String>();
		log.debug(this.collectObjInfo + " : 启动接入线程接收M2000北向告警字符流.");
		Accesser a = new Accesser("Accesser", this.collectObjInfo.getDevInfo().getIP(), this.collectObjInfo.getDevPort());
		a.start();
		log.debug(this.collectObjInfo + " : 启动解析线程解析M2000北向告警字符流.");
		SocketDataParser sdp = new SocketDataParser("Parser");
		sdp.start();
	}

	public boolean isContainFlag(String str, String containFlag)
	{
		boolean flag = false;
		str = str.trim();
		if ((str == null) || ("".equals(str))) return false;
		String[] keyAndValue = str.split("=");
		if (keyAndValue.length > 0)
		{
			if (keyAndValue[0].trim().equals(containFlag))
			{
				flag = true;
			}
		}
		return flag;
	}

	public String getValue(String lineData)
	{
		if ((lineData == null) || (lineData.equals(""))) return "";
    	return lineData.substring(lineData.indexOf("=") + 1).trim();
	}

	public String handSql(String[] msg)
	{
		String value = null;
		StringBuffer tableHead = new StringBuffer();
		StringBuffer filed = new StringBuffer();
		String pitchValue = null;
		StringBuffer sbSql = new StringBuffer();
		int deviceID = this.collectObjInfo.getDevInfo().getDevID();

		for (String lineData : msg)
		{
			lineData = lineData.trim();

			if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_HANDSHAKE))
			{
				return null;
			}

			if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_NUM))
			{
				tableHead.append(",ALARMNUM");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? Integer.valueOf(0) : value));
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_NUM))
			{
				tableHead.append(",NETWORKNUM");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? Integer.valueOf(0) : value));
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.OBJECT_IDCODE))
			{
				tableHead.append(",OBJECTIDCODE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.OBJECT_NAME))
			{
				tableHead.append(",OBJECTNAME");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.OBJECT_TYPE))
			{
				tableHead.append(",OBJECTTYPE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) +  "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_IDCODE))
			{
				tableHead.append(",NETWORKIDCODE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_NAME))
			{
				tableHead.append(",NETWORKNAME");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_TYPE))
			{
				tableHead.append(",NETWORKTYPE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) +  "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_ID))
			{
				tableHead.append(",ALARMID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? Integer.valueOf(0) : value));
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_NAME))
			{
				tableHead.append(",ALARMNAME");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) +  "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_CLASS_ID))
			{
				tableHead.append(",ALARMCLASSID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? Integer.valueOf(0) : value));
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_CLASS))
			{
				tableHead.append(",ALARMCLASS");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_LEVEL_ID))
			{
				tableHead.append(",ALARMLEVELID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? Integer.valueOf(0) : value));
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_LEVEL))
			{
				tableHead.append(",ALARMLEVEL");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_STATUS))
			{
				tableHead.append(",ALARMSTATUS");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) +  "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_TYPE_ID))
			{
				tableHead.append(",ALARMTYPEID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? Integer.valueOf(0) : value));
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_TYPE))
			{
				tableHead.append(",ALARMTYPE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) +  "'");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.BEGIN_TIME))
			{
				tableHead.append(",BEGINTIME");
				value = getValue(lineData);
				filed.append(",to_date('" + (
						value.length() == 0 ? this.defaultDate : value) + "','yyyy-MM-dd HH24:mi:ss')");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.RESUME_DATE))
			{
				tableHead.append(",RESUMEDATE");
				value = getValue(lineData);
				filed.append(",to_date('" + (
						value.length() == 0 ? this.defaultDate : value) + 
				"','yyyy-MM-dd HH24:mi:ss')");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.CONFIRM_DATE))
			{
				tableHead.append(",CONFIRMDATE");
				value = getValue(lineData);
				filed.append(",to_date('" + (
						value.length() == 0 ? this.defaultDate : value) + 
				"','yyyy-MM-dd HH24:mi:ss')");
			}
			else if (isContainFlag(lineData, M2000AlarmTempletP.PITCH_INFO))
			{
				tableHead.append(",PITCHINFO");
				pitchValue = lineData.substring(lineData.indexOf("=") + 1);
				filed.append(",'" + (
						value.length() == 0 ? "" : pitchValue.trim()) + 
				"'");
			} else {
				if (!isContainFlag(lineData, M2000AlarmTempletP.OPERATOR))
					continue;
				tableHead.append(",OPERATOR");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + 
				"'");
			}

		}

		sbSql.append("insert into CLT_HW_M2000_ALARM_CHAR_STREAM(");
    	sbSql.append("OMCID,");
    	sbSql.append("COLLECTTIME,");
    	sbSql.append("STAMPTIME");

    	sbSql.append(tableHead);
    	sbSql.append(")values(");

    	sbSql.append(String.format("%d", deviceID) + ",");
    	sbSql.append("sysdate,");
    	sbSql.append("sysdate");

    	sbSql.append(filed);
    	sbSql.append(")");
    	String sql = sbSql.toString();
    	sbSql.delete(0, sbSql.length());
    	return sql;
	}

	private void executeBatch(List<String> inserts)
	{
		Connection connection = DbPool.getConn();
		Statement statement = null;
		try
		{
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			for (String sql : inserts)
			{
				statement.addBatch(sql);
			}
			statement.executeBatch();
			connection.commit();
		}
		catch (Exception e)
		{
			if (connection != null)
			{
				try
				{
					connection.rollback();
				}
				catch (SQLException localSQLException1)
				{
				}
			}
			errorlog.error(this.collectObjInfo + ":插入数据时出现异常", e);
			try
			{
				if (statement != null)
				{
					statement.close();
				}
				if (connection != null)
				{
					connection.close();
				}
			}
			catch (Exception localException1)
			{
			}
		}
		finally
		{
			try
			{
				if (statement != null)
				{
					statement.close();
				}
				if (connection != null)
				{
					connection.close();
				}
			}
			catch (Exception localException2)
			{
			}
		}
	}

	public static void main(String[] args)
		throws Exception
    {
    }

	public boolean parseData()
	{
		return true;
	}

	class Accesser extends Thread
	{
	    String ip;
	    int port;
	    InputStream in;

	    public Accesser(String name, String ip, int port)
	    {
	    	super();
	    	try
	    	{
		        this.ip = ip;
		        this.port = port;
		        SocketCollect.server = new Socket(ip, port);
		        this.in = SocketCollect.server.getInputStream();
	    	}
	    	catch (IOException e)
	    	{
	    		errorlog.error(SocketCollect.this.collectObjInfo + " : 接入线程出现异常", e);
	    	}
	    }

	    private boolean reAccess()
	    {
	    	boolean b = false;
	    	try
	    	{
	    		SocketCollect.server = new Socket(this.ip, this.port);
	    		this.in = SocketCollect.server.getInputStream();
	    		b = true;
	    	}
	    	catch (IOException e)
	    	{
	    		errorlog.error(SocketCollect.this.collectObjInfo + " : 重新接入线程出现异常", e);
	    	}
	    	return b;
	    }

	    public void run()
	    {
	    	while (true)
	    	try
	    	{
	    		if ((this.in != null) && (!SocketCollect.server.isInputShutdown()) && 
	    				(!SocketCollect.server.isClosed()) && (SocketCollect.server.isConnected()))
	    			continue;
	    		reAccess();
	    		errorlog.debug(SocketCollect.this.collectObjInfo + " ------------------ reAccess ----------------------");

	    		synchronized (SocketCollect.this.buffer)
	    		{
	    			//continue;

	    			SocketCollect.this.buffer.notifyAll();
	    			//continue;
	    			SocketCollect.this.buffer.wait();

	    			if (SocketCollect.this.readCount > 0)
	    				continue;
	    			if ((SocketCollect.this.readCount = this.in.read(SocketCollect.this.buffer)) != -1)
	    			{
	    				continue;
	    			}
	    		}
	    	}
	    	catch (SocketException se)
	    	{
	    		errorlog.error(SocketCollect.this.collectObjInfo + "------------连接拒绝，或者服务器关闭，或者连接失败。----------");
	    		try
	    		{
	    			Thread.sleep(2000L);
	    		}
	    		catch (InterruptedException localInterruptedException)
	    		{
	    		}

	    		boolean b = reAccess();
	    		if (b) {
	    			errorlog.debug(SocketCollect.this.collectObjInfo + "------------------ reAccess succ at exception----------------------"); continue;
	    		}
	    	}
	    	catch (Exception e) {
	    		e.printStackTrace();
	    		try
	    		{
	    			Thread.sleep(2000L);
	    		}
	    		catch (InterruptedException localInterruptedException1)
	    		{
	    		}
	    	}
	    }
	}

	class SocketDataParser extends Thread
	{
		byte[] remainingBytes = null;

		private boolean flag = true;

		public SocketDataParser(String name)
		{
			super();
		}

		synchronized boolean getFlag()
		{
			return this.flag;
		}

		synchronized void shutdown()
		{
			this.flag = false;
		}

		public void clearBuffer()
		{
			SocketCollect.this.readCount = 0;
		}

		private void handleData() throws InterruptedException
		{
			byte[] bytes = (byte[])null;
			synchronized (SocketCollect.this.buffer)
			{
				while (SocketCollect.this.readCount <= 0) {
					SocketCollect.this.buffer.wait();
				}

				//取出数据
				bytes = new byte[SocketCollect.this.readCount];
				System.arraycopy(SocketCollect.this.buffer, 0, bytes, 0, SocketCollect.this.readCount);

				clearBuffer();

				SocketCollect.this.buffer.notifyAll();
			}

			parse(bytes);

			if ((SocketCollect.this.sqlList != null) && (SocketCollect.this.sqlList.size() > 0))
			{
				distribute(SocketCollect.this.sqlList);
			}
		}

		//解析数据
		private void parse(byte[] data)
		{
			String sql = null;
			if (data == null) {
				return;
			}

			byte[] allData = data;
			int len = data.length;
			if (this.remainingBytes != null)
			{
				int rLen = this.remainingBytes.length;
				int newLen = len + rLen;
				allData = new byte[newLen];
				System.arraycopy(this.remainingBytes, 0, allData, 0, rLen);
				System.arraycopy(data, 0, allData, rLen, len);
			}

			boolean startFound = false;
			boolean endFound = false;

			
			//解析数据过程
			
			int startPos = 0;
			int endPos = 0;
			int aLen = allData.length;
			for (int i = 0; i < aLen; i++)
			{
				byte b = allData[i];
				char c = (char)b;

				if (c != '<')
					continue;
				if (i + 4 >= aLen)
					break;
				if ((allData[(i + 1)] == 43) && (allData[(i + 2)] == 43) && 
						(allData[(i + 3)] == 43) && (allData[(i + 4)] == 62))
				{
					startFound = true;
					startPos = i + 4;
					i = startPos;
				}
				else if ((allData[(i + 1)] == 45) && (allData[(i + 2)] == 45) && 
						(allData[(i + 3)] == 45) && (allData[(i + 4)] == 62) && 
						(startFound))
				{
					endFound = true;
					endPos = i + 4;
					i = endPos;
				}
				
				if ((!startFound) || (!endFound))
					continue;
				int mLen = endPos - startPos + 4 + 1;
				byte[] msgBytes = new byte[mLen];
				System.arraycopy(allData, startPos - 4, msgBytes, 0, mLen);
				startFound = false;
				endFound = false;
				
				sql = parseMsg(msgBytes);
				if (sql == null)
					continue;
				SocketCollect.this.sqlList.add(sql);
			}

			int remainingLen = aLen - endPos;
			this.remainingBytes = new byte[remainingLen];
			System.arraycopy(allData, endPos, this.remainingBytes, 0, remainingLen);
		}

		private String parseMsg(byte[] msgBytes)
		{
			String msg = new String(msgBytes);
			String[] msgTemp = msg.split("\r\n");
			String sql = SocketCollect.this.handSql(msgTemp);

			return sql;
		}

		public void distribute(List<String> sqlList)
		{
			SocketCollect.this.executeBatch(sqlList);
			sqlList.clear();
		}

		public void run()
		{
			while (getFlag())
			{
				try
				{
					handleData();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}