package com.turk.access;

import com.turk.Config.SystemConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.distributor.Distribute;
import com.turk.distributor.DistributeFile;
import com.turk.distributor.DistributeSqlLdr;
import com.turk.distributor.DistributeTemplet;
import com.turk.distributor.TableItem;
import com.turk.parser.Parser;
import com.turk.task.CollectObjInfo;
import com.turk.task.RegatherObjInfo;
import com.turk.task.TaskMgr;
import com.turk.templet.GenericSectionHeadD;
import com.turk.templet.TempletBase;
import com.turk.util.LogMgr;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * �ɼ����ʳ�����
 * @author Administrator 
 *
 */
public abstract class AbstractAccessor extends Task
  implements Accessor
{
	protected Logger log = LogMgr.getInstance().getSystemLogger();
	protected Logger errorlog = LogMgr.getInstance().getErrorLogger();
	
	protected CollectObjInfo taskInfo;
	protected Parser parser;
	protected Distribute distributor;
	protected boolean runFlag = true;

	private boolean accessSucc = false;
	private int taskID;
	protected String strLastGatherTime;
	private GenericDataConfig dataSourceConfig;
	protected String name;

	public abstract void configure()
    	throws Exception;

	public abstract boolean access()
    	throws Exception;

	public void shutdown()
	{
	}

	public void dispose(long lastCollectTime)
	{
		closeFiles();

		this.runFlag = false;
		this.taskInfo.setUsed(false);

		String logStr = this.name + ": remove from active-task-map. " + 
			this.strLastGatherTime;
		this.log.debug(logStr);
		this.taskInfo.log("����", logStr);
		if(this.taskInfo.getCollectType() != 10)
		{
			TaskMgr.getInstance().delActiveTask(this.taskInfo.getKeyID(), this.taskInfo instanceof RegatherObjInfo);
		}
		else
		{
			//socket ʵʱ�ɼ��ӿ�
			this.log.info("Sokect Start ID��" + this.taskInfo.getKeyID());
		}

		TaskMgr.getInstance().commitRegather(this.taskInfo, lastCollectTime);
		
	}

	/**
	 * ��֤
	 */
	public boolean validate()
	{
		boolean b = true;

		if (this.taskInfo == null) {
			return false;
		}

		if (((this.dataSourceConfig == null) || (this.dataSourceConfig.getDatas() == null)) && 
				(this.taskInfo.getCollectType() != 9))
		{
			this.log.error("taskId-" + this.taskInfo.getTaskID() + 
				":������Ч����ԭ��collect_pathΪ��");
			return false;
		}

		try
		{
			this.strLastGatherTime = Util.getDateString(this.taskInfo.getLastCollectTime());
		}
		catch (Exception e)
		{
			this.errorlog.error(this.name + "> ʱ���ʽ����,ԭ��:", e);
			this.taskInfo.log("��ʼ", this.name + "> ʱ���ʽ����,ԭ��:", e);
			b = false;
		}

		return b;
	}

	/**
	 * ׼��
	 */
	public void doReady()
    	throws Exception
    {
		this.taskInfo.setUsed(true);

		this.taskInfo.startTime = new Timestamp(new Date().getTime());

		int sleepTime = this.taskInfo.getThreadSleepTime();
		if (sleepTime > 0)
		{
			this.log.debug(this.name + " sleep " + sleepTime + " (s)");
			this.taskInfo.log("��ʼ", this.name + " sleep " + sleepTime + 
				" (s)");
			Thread.sleep(sleepTime * 1000);
		}
    }

	/**
	 * �ɼ�����ʼ
	 */
	public void doStart()
    	throws Exception
    {
		this.log.info(this.name + ": ��ʼ�ɼ�ʱ���Ϊ " + this.strLastGatherTime + " ������.");
		this.taskInfo.log("��ʼ", this.name + ": ��ʼ�ɼ�ʱ���Ϊ " + 
				this.strLastGatherTime + " ������.");
    }

	public boolean doBeforeAccess()
    	throws Exception
    {
		return true;
    }

	/**
	 * ����
	 */
	public void parse(char[] chData, int iLen)
    	throws Exception
    {
    }

	public boolean doAfterAccess()
      	throws Exception
    {
		return true;
    }

    public void doFinishedAccess()
    	throws Exception
    {
    }

    public void doSqlLoad()
    	throws Exception
    {
    	String logStr = null;

    	if (this.accessSucc)
    	{
    		//DistributeTemplet disTmp = ((DistributeTemplet)taskInfo.getDistributeTemplet());
    		
    		int parseType = this.taskInfo.getParseTmpType();
    		//this.taskInfo.get
    		if ((parseType != 3) && 
    				(parseType != 0) && 
    				(this.taskInfo.getParserID() != 35) && parseType != -1)
    		{
    			File ldrlogDirectory = new File(SystemConfig.getInstance().getCurrentPath() + 
    					File.separator + "ldrlog");
    			if (!ldrlogDirectory.exists())
    			{
    				ldrlogDirectory.mkdir();
    			}

    			logStr = this.name + ": " + this.strLastGatherTime + " Load data start.";
    			this.log.info(logStr);
    			this.taskInfo.log("���", logStr);

    			runSqlldr(true);
    		}
    	

    		logStr = this.name + ": " + this.strLastGatherTime + " Load data end.";
    		this.log.info(logStr);
    		this.taskInfo.log("����", logStr);

    		logStr = this.name + ": " + this.taskInfo.getDescribe() + " import " + 
    			this.strLastGatherTime + " finish ����ʼʱ��:" + 
    			this.taskInfo.startTime + " " + this.taskInfo.m_nAllRecordCount;
    		this.log.info(logStr);
    		this.taskInfo.log("����", logStr);
    	}
    }

    /**
     * �ɼ��������
     */
    public void doFinished()
    	throws Exception
    {
    	if (this.taskInfo.getPeriod() == 1)
    	{
    		this.runFlag = true;
    		while (true)
    		{
    			Thread.sleep(5000L);
    		}
    	}

    	this.runFlag = false;
    	
    	this.taskInfo.doAfterCollect();
    }

  	public void run()
  	{
  		String logStr = null;
  		long lastCollectTime = -1L;
  		try { 
  			configure();

  			boolean b = validate();
  			if (!b) return;
  			do { 

  				doReady();
  				doStart();
  				
  				b = doBeforeAccess(); 
  			} while (!b);

  			lastCollectTime = this.taskInfo.getLastCollectTime().getTime();
  			this.accessSucc = (b = access());
  			logStr = this.name + ": ����(" + this.strLastGatherTime + ")�������. ������=" + 
  				this.accessSucc;
  			this.log.info(logStr);
  			this.taskInfo.log("��ʼ", logStr);

  			b = doAfterAccess();

  			doFinishedAccess();

  			doSqlLoad();

  			doFinished();

  			this.accessSucc = true;

  			Thread.sleep(3000L);
  		}
  		catch (InterruptedException ie)
    	{
    		this.log.error(this.name + ": ���������ֹ");
    		this.taskInfo.log("����", this.name + ": ���������ֹ", ie);
    	}
    	catch (Exception e)
    	{
    		logStr = this.name + ": ����(" + this.strLastGatherTime + ")�����쳣,ԭ��";
    		this.errorlog.error(logStr, e);
    		this.taskInfo.log("����", logStr, e);
    	}
    	finally
    	{
    		dispose(lastCollectTime);
    	}
  	}

  	private void runSqlldr(boolean isAll)
  	{
  		boolean isRedoFlag = false;
  		isRedoFlag = TaskMgr.getInstance().isReAdoptObj(this.taskInfo);

  		DistributeSqlLdr sqlldr = new DistributeSqlLdr(this.taskInfo);
  		DistributeFile outputfile = new DistributeFile(this.taskInfo);
  		
  		DistributeTemplet distmp = (DistributeTemplet)this.taskInfo.getDistributeTemplet();

  		if ((distmp == null) || (distmp.tableTemplets == null)) {
  			return;
  		}
  		for (int i = 0; i < distmp.tableTemplets.size(); i++)
  		{
  			if ((!isAll) && 
  					(this.taskInfo.getActiveTableIndex() != i)) {
  				continue;
  			}
  			TableItem tableItem = (TableItem)distmp.tableItems.get(Integer.valueOf(i));

  			DistributeTemplet.TableTemplet table = (DistributeTemplet.TableTemplet)distmp.tableTemplets.get(Integer.valueOf(i));

  			String strOldFileName = tableItem.fileName;
  			FileOutputStream fw = tableItem.fileWriter;
  			try
  			{
  				if (fw == null) continue;
  				fw.close();
  			}
  			catch (IOException e)
  			{
  				this.errorlog.error(this + ": runSqlldr", e);
  				this.taskInfo.log("���", this + ": runSqlldr", e);
  			}

  			if (isAll)
  			{
  				if (isRedoFlag)
  				{
  					RegatherObjInfo rTask = (RegatherObjInfo)this.taskInfo;
  					if ((!rTask.isEmptyTableIndexes()) && 
  							(!((RegatherObjInfo)this.taskInfo).existsInTableIndexes(i)))
  					{
  						continue;
  					}

  				}
  				
  			}

  			switch(distmp.stockStyle)
  			{
  				case 0:
  					break;
	  			case 1://Insert Sql
	  				break;
	  			case 2://SQLLOAD 
	  				sqlldr.buildSqlLdr(table.tableIndex, strOldFileName);
	  			case 3:
	  				break;
	  			case 4: //FILE
	  				outputfile.BuildFileUploadFtp(table.tableIndex, strOldFileName);
	  				break;
  			}
  			

  			tableItem.recordCounts = 0;

  			if (isAll)
  				continue;
  			String strOracleCurrentPath = SystemConfig.getInstance().getCurrentPath();

  			Date now = new Date(this.taskInfo.getLastCollectTime().getTime());
  			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
  			String strTime = formatter.format(now);
  			String strNewFileName = this.taskInfo.getGroupId() + "_" + 
  				this.taskInfo.getTaskID() + "_" + strTime + "_" + 
  				String.valueOf(i);
  			tableItem.fileName = strNewFileName;
  			try
  			{
  				fw = new FileOutputStream(strOracleCurrentPath + 
  						File.separatorChar + strNewFileName + ".txt");
  				tableItem.fileWriter = fw;
        
  				if (!table.isFillTitle)
  					continue;
  				try
  				{
  					for (int k1 = 0; k1 < table.fields.size(); k1++)
  					{
  						DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)table.fields.get(Integer.valueOf(k1));

  						if (k1 < table.fields.size() - 1)
  							fw.write(String.format("%s;", field.m_strFieldName).getBytes());
  						else
  							fw.write(field.m_strFieldName.getBytes());
  					}
  					fw.write("\n".getBytes());
  					fw.flush();
  				}
  				catch (IOException e)
  				{
  					this.errorlog.error(this.name + ": runSqlldr", e);
  					this.taskInfo.log("���", this.name + 
  							": runSqlldr", e);
  				}

  			}
  			catch (Exception e)
  			{	
  				this.errorlog.error(this.name + ": runSqlldr", e);
  				this.taskInfo.log("���", this.name + ": runSqlldr", e);
  			}
  		}
  	}

  	/**
  	 * �ر��ļ�
  	 */
  	private void closeFiles()
  	{
  		TempletBase distmp = this.taskInfo.getDistributeTemplet();
  		if (!(distmp instanceof GenericSectionHeadD))
  		{
  			for (int i = 0; i < ((DistributeTemplet)distmp).tableTemplets.size(); i++)
  			{
  				TableItem tableItem = (TableItem)((DistributeTemplet)distmp).tableItems.get(Integer.valueOf(i));
  				if (tableItem == null) {
  					continue;
  				}
  				FileOutputStream fw = tableItem.fileWriter;
  				if (fw == null)
  					continue;
  				try
  				{
  					fw.close();
  				}
  				catch (IOException localIOException)
  				{
  				}
  			}
  		}
  	}

  	public CollectObjInfo getTaskInfo()
  	{
  		return this.taskInfo;
  	}

  	public void setTaskInfo(CollectObjInfo obj)
  	{
  		this.taskInfo = obj;
  		this.taskID = obj.getTaskID();
  		this.dataSourceConfig = GenericDataConfig.wrap(obj.getCollectPath());
    
  		int id = obj.getTaskID();
  		if ((obj instanceof RegatherObjInfo))
  			id = obj.getKeyID() - 10000000;
  		this.name = (obj.getTaskID() + "-" + id);
  	}

  	public Parser getParser()
  	{
  		return this.parser;
  	}

  	public void setParser(Parser parser)
  	{
  		this.parser = parser;
  	}

  	public Distribute getDistributor()
  	{
  		return this.distributor;
  	}

  	public void setDistributor(Distribute distributor)
  	{
  		this.distributor = distributor;
  	}

  	public int getTaskID()
  	{
  		return this.taskID;
  	}

  	public void setTaskID(int taskID)
  	{
  		this.taskID = taskID;
  	}

  	public GenericDataConfig getDataSourceConfig()
  	{
  		return this.dataSourceConfig;
  	}

  	public void setDataSourceConfig(GenericDataConfig dataSourceConfig)
  	{
  		this.dataSourceConfig = dataSourceConfig;
  	}

  	public String getStrLastGatherTime()
  	{
  		return this.strLastGatherTime;
  	}

  	public void setStrLastGatherTime(String strLastGatherTime)
  	{
  		this.strLastGatherTime = strLastGatherTime;
  	}	

  	public boolean isAccessSucc()
  	{
  		return this.accessSucc;
  	}

  	public void setAccessSucc(boolean accessSucc)
  	{
  		this.accessSucc = accessSucc;
  	}

  	public String getMyName()
  	{
  		return this.name;
  	}
  	
  	public String toString()
  	{
  		return "Thread:"+taskInfo.getDescribe();
  	}
}