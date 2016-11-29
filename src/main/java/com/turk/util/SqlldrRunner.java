package com.turk.util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;

//import task.CollectObjInfo;
//import task.DevInfo;
import com.turk.util.loganalyzer.SqlLdrLogAnalyzer;

public class SqlldrRunner
{
  private ExternalCmd executor;
  private String serviceName;
  private String userName;
  private String password;
  private String cltPath;
  private String badpath;
  private String logPath;
  private int skip;
  private SqlldrResult result;
  private static Logger logger = LogMgr.getInstance().getSystemLogger();

  private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

  public SqlldrRunner()
  {
  }

  public SqlldrRunner(String serviceName, String userName, String password, String cltPath, String badpath, String logPath, int skip)
  {
    this();
    this.serviceName = serviceName;
    this.userName = userName;
    this.password = password;
    this.cltPath = cltPath;
    this.badpath = badpath;
    this.logPath = logPath;
    this.skip = skip;
  }

  public void runSqlldr(int taskID,int keyID,int deviceID,Date LastCollectTime)
  {
    String cmd = "sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=999999";
    cmd = String.format(cmd, new Object[] { this.userName, this.password, this.serviceName, Integer.valueOf(this.skip), this.cltPath, this.badpath, this.logPath });
    logger.debug("Ҫִ�е�sqlldr����Ϊ��" + 
      cmd.replace(this.userName, "*").replace(this.password, "*"));

    this.executor = new ExternalCmd();
    this.executor.setCmd(cmd);

    int retCode = -1;
    try
    {
      retCode = this.executor.execute();
      if ((retCode == 0) || (retCode == 2))
      {
        logger.debug("Task-" + taskID + "-" + keyID + 
          ": sqldr OK. retCode=" + retCode);
      }
      else if ((retCode != 0) && (retCode != 2))
      {
        int maxTryTimes = 3;
        int tryTimes = 0;
        long waitTimeout = 30000L;
        while (tryTimes < maxTryTimes)
        {
          retCode = this.executor.execute();
          if ((retCode == 0) || (retCode == 2))
          {
            break;
          }

          tryTimes++;
          waitTimeout *= 2L;

          logger.error("Task-" + taskID + "-" + keyID + ": ��" + 
            tryTimes + "��Sqlldr�������ʧ��. " + cmd + " retCode=" + 
            retCode);

          Thread.sleep(waitTimeout);
        }

        if ((retCode == 0) || (retCode == 2))
        {
          logger.info("Task-" + taskID + "-" + keyID + ": " + 
            tryTimes + "��Sqlldr��������ɹ�. retCode=" + retCode);
        }
        else
        {
          logger.error("Task-" + taskID + "-" + keyID + " : " + 
            tryTimes + "��Sqlldr�������ʧ��. " + cmd + " retCode=" + 
            retCode);
        }
      }
    }
    catch (Exception e)
    {
      logger.error("ִ��sqlldrʱ�����쳣��ԭ�� " + e.getMessage());
    }

    File logFile = new File(this.logPath);
    if ((!logFile.exists()) || (!logFile.isFile()))
    {
      logger.info(this.logPath + "�����ڣ�����ID��" + taskID);
      return;
    }
    SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
    try
    {
      SqlldrResult result = analyzer.analysis(new FileInputStream(this.logPath));
      if (result == null) {
        return;
      }
      logger.debug("Task-" + taskID + "-" + keyID + 
        ": SQLLDR��־�������: DeviceID=" + 
        deviceID + " ����=" + 
        result.getTableName() + " ����ʱ��=" + 
        Util.getDateString(LastCollectTime) + 
        "�ļ����ʱ��=" + result.getRunTime() + "(s) ���ɹ�����=" 
        + result.getLoadSuccCount() + " sqlldr��־=" + 
        this.logPath);

       dbLogger.log(deviceID, result.getTableName(), LastCollectTime.getTime(), 
    		   result.getLoadSuccCount(), taskID, result.getRunTime());
      if (SystemConfig.getInstance().isDeleteLog())
      {
        new File(this.badpath).delete();
        new File(this.cltPath).delete();
        new File(this.badpath.replace(".bad", ".txt")).delete();
        new File(this.logPath).delete();
      }
    }
    catch (Exception e)
    {
      logger.error("Task-" + taskID + "-" + keyID + ": sqlldr��־����ʧ�ܣ��ļ�����" + 
        this.logPath + "��ԭ��: ", e);
    }
  }

  public ExternalCmd getExecutor()
  {
    return this.executor;
  }

  public void setExecutor(ExternalCmd executor)
  {
    this.executor = executor;
  }

  public String getServiceName()
  {
    return this.serviceName;
  }

  public void setServiceName(String serviceName)
  {
    this.serviceName = serviceName;
  }

  public String getUserName()
  {
    return this.userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassword()
  {
    return this.password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public String getCltPath()
  {
    return this.cltPath;
  }

  public void setCltPath(String cltPath)
  {
    this.cltPath = cltPath;
  }

  public SqlldrResult getResult()
  {
    return this.result;
  }

  public String getBadpath()
  {
    return this.badpath;
  }

  public void setBadpath(String badpath)
  {
    this.badpath = badpath;
  }

  public String getLogPath()
  {
    return this.logPath;
  }

  public void setLogPath(String logPath)
  {
    this.logPath = logPath;
  }

  public void delLogs()
  {
    new File(this.logPath).delete();
    new File(this.badpath).delete();
    new File(this.cltPath).delete();
  }

  public void printResult(SqlldrResult result)
  {
    logger.info("===============sqlldr�������=================");

    logger.info("��־λ�ã�" + this.logPath);
    logger.info("������" + result.getTableName());
    logger.info("����ɹ���������" + result.getLoadSuccCount());
    logger.info("�����ݴ����û�м��ص�������" + result.getData());
    logger.info("��when�Ӿ�ʧ��ҳû�м��ص�������" + result.getWhen());
    logger.info("null�ֶ�������" + result.getNullField());
    logger.info("�������߼���¼������" + result.getSkip());
    logger.info("��ȡ���߼���¼������" + result.getRead());
    logger.info("�ܾ����߼���¼������" + result.getRefuse());
    logger.info("�������߼���¼������" + result.getAbandon());
    logger.info("��ʼ����ʱ�䣺" + result.getStartTime());
    logger.info("��������ʱ�䣺" + result.getEndTime());

    logger.info("==============================================");
  }
}