package com.turk.framework;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import com.turk.Config.SystemConfig;
import com.turk.util.Util;

public class DataLifecycleMgr
{
  private boolean enable = false;
  private boolean working = false;
  private boolean DeleteWhenOff = true;

  private String rootPath = "";
  private int fileLifecycle;
  private String fileExtName;
  private Work workHandler;
  private static DataLifecycleMgr mgr = null;
  private static Logger logger = Logger.getLogger(DataLifecycleMgr.class);
  private DataLifecycleMgr()
  {
    try
    {
      init();
    }
    catch (Exception e)
    {
      logger.error("数据文件存活时间管理模块加载失败,原因:加载配置文件失败. 堆栈:", e);
    }
  }

  public static synchronized DataLifecycleMgr getInstance()
  {
    if (mgr == null)
    {
      mgr = new DataLifecycleMgr();
    }
    return mgr;
  }

  public void start()
  {
    if ((this.enable) && (!isWorking()))
    {
      logger.info("开启扫描时间戳文件线程");
      this.workHandler = new Work("DataLifecycleMgr-work-thrd");
      this.workHandler.start();
    }
  }

  public void stop()
  {
    if (this.workHandler != null)
    {
      this.workHandler.shutdown();
      this.workHandler = null;
    }
  }

  private void init()
  {
    this.rootPath = SystemConfig.getInstance().getCurrentPath().trim();
    if (Util.isNull(this.rootPath))
    {
      logger.debug("文件根目录不存在异常.");
      return;
    }

    Integer filePeriod = SystemConfig.getInstance().getFilecycle();
    try
    {
      this.fileLifecycle = filePeriod;
    }
    catch (NumberFormatException e)
    {
      this.fileLifecycle = 10080;
    }
    if (this.fileLifecycle < 0) {
      this.fileLifecycle = 10080;
    }

    this.fileExtName = SystemConfig.getInstance().getLifecycleFileExt();
    if (Util.isNull(this.fileExtName))
    {
      this.fileExtName = ".uway_ts";
    }

    if (SystemConfig.getInstance().isEnableDataFileLifecycle()) {
      this.enable = true;
    }

    if (!SystemConfig.getInstance().isDeleteWhenOff())
      this.DeleteWhenOff = false;
  }

  public synchronized boolean isEnable()
  {
    return this.enable;
  }

  public void doFileTimestamp(String filePath, Date dataTime)
  {
    if (!isEnable())
    {
      return;
    }

    if (!isWorking())
    {
      start();
    }

    if (dataTime == null) {
      return;
    }

    String collectDataTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

    String nowTime = Util.getDateString_yyyyMMddHHmmss(new Date());

    String fileTimePostfix = filePath + "###" + collectDataTime + "###" + 
      nowTime + this.fileExtName;
    File fileTime = new File(fileTimePostfix);
    if (!fileTime.exists())
    {
      try
      {
        fileTime.createNewFile();
      }
      catch (IOException e)
      {
        logger.error("创建时间戳文件失败.原因:", e);
      }
    }
  }

  public boolean isDeleteWhenOff()
  {
    return this.DeleteWhenOff;
  }

  public synchronized boolean isWorking()
  {
    return this.working;
  }

  public synchronized void setWorking(boolean working)
  {
    this.working = working;
  }

  public static void main(String[] args)
  {
  }

  class Work extends Thread
  {
    boolean flag = true;

    public Work()
    {
    }

    public Work(String thrdName)
    {
      super();
    }

    synchronized boolean isRunning()
    {
      return this.flag;
    }

    synchronized void shutdown()
    {
      this.flag = false;
    }

    private List<File> loadFilename(File file)
    {
      if (file == null) {
        return null;
      }
      List<File> filenameList = new ArrayList<File>();
      if (file.isFile())
      {
        String path = file.getPath();
        if (path.endsWith(DataLifecycleMgr.this.fileExtName))
        {
          filenameList.add(file);
        }
      }
      if (file.isDirectory())
      {
        for (File f : file.listFiles())
        {
          if (f == null)
            continue;
          filenameList.addAll(loadFilename(f));
        }
      }
      return filenameList;
    }

    private String getFileNameExcludeExt(String path)
    {
      if (path == null) {
        return null;
      }
      String fileName = null;
      fileName = path.substring(path.lastIndexOf(File.separator) + 1, path.lastIndexOf("."));
      return fileName;
    }

    private void compareDate()
    {
      File file = new File(DataLifecycleMgr.this.rootPath);
      List<File> lst = loadFilename(file);

      for (File f : lst)
      {
        String tsFilepath = f.getPath();
        String fileName = getFileNameExcludeExt(tsFilepath);
        String folderPath = tsFilepath.substring(0, tsFilepath.lastIndexOf(File.separator));

        String[] strPath = fileName.split("###");
        if (strPath.length != 3) {
          continue;
        }
        String rawFilePath = folderPath + File.separator + strPath[0];

        String downFileTime = strPath[2];

        Date colleDate = null;
        try
        {
          colleDate = Util.getDate2(downFileTime);
        }
        catch (ParseException e)
        {
          continue;
        }
        if (colleDate == null)
        {
          continue;
        }
        long times = colleDate.getTime() + DataLifecycleMgr.this.fileLifecycle * 60 * 1000;

        long now = new Date().getTime();
        if (times > now)
          continue;
        File oldfile = new File(rawFilePath);
        String strFlag = oldfile.delete() ? "成功" : "失败";
        DataLifecycleMgr.logger.debug("文件存活周期已到,删除原始文件：" + rawFilePath + " --" + 
          strFlag);

        File flagfile = new File(tsFilepath);
        strFlag = flagfile.delete() ? "成功" : "失败";
        DataLifecycleMgr.logger.debug("文件存活周期已到,删除时间戳文件：" + tsFilepath + " --" + 
          strFlag);

        File ctlfile = null;

        String dataFileName = rawFilePath.substring(rawFilePath.lastIndexOf("\\") + 1, rawFilePath.length());
        if (dataFileName.contains("."))
          ctlfile = new File(rawFilePath.substring(0, rawFilePath.lastIndexOf(".")) + 
            ".ctl");
        else {
          ctlfile = new File(rawFilePath + ".ctl");
        }

        if (!ctlfile.exists())
          continue;
        strFlag = ctlfile.delete() ? "成功" : "失败";
        DataLifecycleMgr.logger.debug(ctlfile.getPath() + " 删除" + strFlag);
      }
    }

    public void run()
    {
      DataLifecycleMgr.this.setWorking(true);

      while (isRunning())
      {
        try
        {
          compareDate();
        }
        catch (Exception e)
        {
          DataLifecycleMgr.logger.error("删除时间戳文件时错误", e);
        }

        try
        {
          Thread.sleep(2000L);
        }
        catch (InterruptedException localInterruptedException)
        {
        }
      }

      DataLifecycleMgr.this.setWorking(false);
    }
  }
}