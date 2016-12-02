package com.turk.store;

import com.turk.templet.Table;

public class SqlldrStoreParam
{
  private int templetID;
  private Table table;

  public SqlldrStoreParam()
  {
  }

  public SqlldrStoreParam(int templetID, Table table)
  {
    this.templetID = templetID;
    this.table = table;
  }

  public int getTempletID()
  {
    return this.templetID;
  }

  public void setTempletID(int templetID)
  {
    this.templetID = templetID;
  }

  public Table getTable()
  {
    return this.table;
  }

  public void setTable(Table table)
  {
    this.table = table;
  }
}