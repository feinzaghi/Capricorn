package com.turk.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;

import org.apache.log4j.Logger;
//import task.CollectObjInfo;




import com.turk.config.SystemConfig;
import com.turk.util.loganalyzer.SqlLdrLogAnalyzer;

public class BalySqlloadThread extends Task
{
	private String execcmd;
	private int tableIndex;
	private int time = -1;
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();
	private int recordNum = 0;
	private String logfilename = ""; //日志文件名
	private String txtfilename = ""; //数据文件名
	private String tablename = "";   //入库对应表名
	private String cltfilename =""; //控制文件名称
	private String timestring = "";
	private int keyid = 0;
	private boolean executeimmediate = false;
	public boolean IsFinish = false;
	
	public void setTime(int time)
	{
		this.time = time;
	}
	
	public int getRecordNum()
	{
		return recordNum;
	}
	
	public void setLogFileName(String filename)
	{
		this.logfilename = filename;
	}
	
	public void setTxtFileName(String filename)
	{
		this.txtfilename = filename;
	}
	
	public void setTableName(String tablename)
	{
		this.tablename = tablename;
	}
	
	public void setTimeString(String timestring)
	{
		this.timestring = timestring;
	}
	
	public void setKeyID(int keyID)
	{
		this.keyid = keyID;
	}
	
	public void setCltFileName(String filename)
	{
		this.cltfilename = filename;
	}
	
	public void setExecuteImmediate(boolean executeimmediate)
	{
		this.executeimmediate = executeimmediate;
	}

	public void run() {
		try
		{
			boolean IsSuccess = false;
			int retCode = runcmd(getExeccmd());
			if ((retCode == 0) || (retCode == 2))
			{
				this.log.debug("Input Database: sqldr OK. retCode=" + 
						retCode);
				if(retCode == 0)
					IsSuccess = true;
				
				if(retCode == 2)
				{
					SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
					SqlldrResult result = analyzer.analysis(new FileInputStream(logfilename));
					this.log.debug("Input Database: Sqldr retCode = 2;" 
							+ "Msg=" + result.getMessage());
					//对于SQL返回信息retCode=2的信息，将日志文件记录下来。
					Util.FileCopy(logfilename, logfilename + ".err");
				}
			}
			else if ((retCode != 0) && (retCode != 2))
			{
				int maxTryTimes = 3;
				int tryTimes = 0;
				long waitTimeout = 30000L;
				while (tryTimes < maxTryTimes)
				{
					retCode = runcmd(execcmd);
					if ((retCode == 0) || (retCode == 2))
					{
						break;
					}

					tryTimes++;
					waitTimeout *= 2L;

					this.log.error("Input Database: 第" + tryTimes + 
							"次Sqlldr尝试入库失败. " + execcmd + " retCode=" + retCode);

					Thread.currentThread(); 
					Thread.sleep(waitTimeout);
				}

				if ((retCode == 0) || (retCode == 2))
				{
					this.log.info("Input Database:  " + tryTimes + 
							"次Sqlldr尝试入库后成功. retCode=" + retCode);
				}
				else
				{
					this.log.error("Input Database:  " + tryTimes + 
							"次Sqlldr尝试入库失败. " + execcmd + " retCode=" + retCode);

				}
			}
			else
			{
				this.log.error("Input Database: sqlldr 失败 并且不重试.");
			}
			
			//日志分析
			File logFile = new File(logfilename);
			if ((!logFile.exists()) || (!logFile.isFile()))
			{
				this.log.info("Log File :" + logfilename + "不存在.");
				return;
			}
			
			SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
			try
			{
				SqlldrResult result = analyzer.analysis(new FileInputStream(logfilename));
				if (result == null) {
					return;
				}
				
				File file = new File(txtfilename);
				double fileBytes = 0;
				if (file.exists()) { 
					FileInputStream fis = null; 
					fis = new FileInputStream(file); 
					fileBytes = fis.available(); 
					fileBytes = (double)fileBytes/(double)1024;
					
					fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
					fis.close();
				}
				
				tablename = result.getTableName();
				this.log.debug("路测: " + tablename + ": SQLLDR日志分析结果:  表名=" + 
						tablename + " 数据时间=" + 
						timestring + "文件大小=" + fileBytes + "KB 文件入库时长" + result.getRunTime() +
						"(s) 入库成功条数=" + result.getLoadSuccCount() + " sqlldr日志=" + 
						logfilename);
				
				
				dbLogger.log(keyid, result.getTableName(), 
						timestring, 
						result.getLoadSuccCount(), keyid,result.getRunTime(),fileBytes);
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
				this.log.error("Input Database: " + tablename + ": sqlldr日志分析失败，文件名：" + 
						logfilename + "，原因: ", e);
			}
			
			if (SystemConfig.getInstance().isDeleteLog())
			{
				File ctlfile = new File(cltfilename);
				if (ctlfile.exists()) {
					ctlfile.delete();
				}

				String strTxt = txtfilename;
				File txtfile = new File(strTxt);
				if (txtfile.exists())
				{
					if (txtfile.delete())
					{
						this.log.debug("Input File" + ": " + strTxt + 
						" delete success....");
					}
					else
					{
						this.log.warn("Input File" + ": " + strTxt + 
						" delete failure");
					}
				}
				else
				{
					this.log.debug("Input File" + ": " + strTxt + 
					" can not found file");
				}
			}
		}
		catch (Exception e)
		{
			log.error("Sqlldr Error",e);
		}
		IsFinish = true;
	}

	public int runcmd(String cmd)
    	throws Exception
    {
		int retvalue = 0;

		Process proc = Runtime.getRuntime().exec(cmd);
		
		StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "Error");
		StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "Output");
		errorGobbler.start();
		outputGobbler.start();
		
		log.debug("waitfor");
		proc.waitFor();
		if (this.time == -1) {
			Thread.sleep(5000L);
		}
		
		/*
		if(cmd.contains("mysql"))
		{
			InputStreamReader isr = new InputStreamReader(proc.getErrorStream());
			BufferedReader br = new BufferedReader(isr);

			String line = null;
			while ((line = br.readLine()) != null);
			log.debug(line);
			try
			{
				line = line.substring(line.indexOf("Records: ")+"Records: ".length());
				line = line.substring(0,line.indexOf(" "));
				recordNum = Integer.parseInt(line);
			}
			catch(Exception ex)
			{
				log.error("读取MYSQL日志异常",ex);
			}
		}
		else
		{
			
		}*/
		
		retvalue = proc.exitValue();
		log.debug("sqlldr.exitvalue=" + retvalue);

		proc.destroy();

		return retvalue;
    }

	public String getExeccmd()
	{
		return this.execcmd;
	}

	public void setExeccmd(String execcmd)
	{
		log.debug(execcmd);
		this.execcmd = execcmd;
	}


	public int getTableIndex()
	{
		return this.tableIndex;
	}

	public void setTableIndex(int tableIndex)
	{
		this.tableIndex = tableIndex;
	}

	class StreamGobbler extends Thread
	{
		InputStream is;
		String type;

		StreamGobbler(InputStream is, String type)
		{
			this.is = is;
			this.type = type;
		}

		public void run()
		{
			try
			{
				InputStreamReader isr = new InputStreamReader(this.is);
				BufferedReader br = new BufferedReader(isr);

				String line = null;
				while ((line = br.readLine()) != null);
				if(line!=null)
					log.debug(line);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		return "执行文件:"+txtfilename;
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return executeimmediate;
	}

	@Override
	public Task taskCore() throws Exception {
		// TODO Auto-generated method stub
		return this;
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