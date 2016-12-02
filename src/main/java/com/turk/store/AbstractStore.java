package com.turk.store;

import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.turk.exception.StoreException;
import com.turk.task.CollectObjInfo;
import com.turk.util.DBLogger;
import com.turk.util.LogMgr;

public class AbstractStore<T>
  implements Store
{
  protected static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();
  protected static Logger log = LogMgr.getInstance().getSystemLogger();
  private T param;
  private int taskID;
  private Timestamp dataTime;
  private int deviceID;
  private String flag;
  private CollectObjInfo collectInfo;

  public AbstractStore()
  {
  }

  public AbstractStore(T param)
  {
    this.param = param;
  }

  public void open()
    throws StoreException
  {
  }

  public void write(String data)
    throws StoreException
  {
  }

  public void flush()
    throws StoreException
  {
  }

  public void commit()
    throws StoreException
  {
  }

  public void close()
  {
  }

  public T getParam()
  {
    return this.param;
  }

  public void setParam(T param)
  {
    this.param = param;
  }

  public int getTaskID()
  {
    return this.taskID;
  }

  public void setTaskID(int taskID)
  {
    this.taskID = taskID;
  }

  public Timestamp getDataTime()
  {
    return this.dataTime;
  }

  public void setDataTime(Timestamp dataTime)
  {
    this.dataTime = dataTime;
  }

  public int getDeviceID()
  {
    return this.deviceID;
  }

  public void setDeviceID(int id)
  {
    this.deviceID = id;
  }

  public CollectObjInfo getCollectInfo()
  {
    return this.collectInfo;
  }

  public void setCollectInfo(CollectObjInfo collectInfo)
  {
    this.collectInfo = collectInfo;
  }

  public String getFlag()
  {
    return this.flag;
  }

  public void setFlag(String flag)
  {
    this.flag = flag;
  }
}