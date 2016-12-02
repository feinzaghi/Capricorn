package com.turk.parser;

import com.turk.distributor.DistributeTemplet;
import com.turk.Config.ConstDef;
import com.turk.Config.SystemConfig;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.turk.exception.StoreException;
import com.turk.store.SqlldrStore;
import com.turk.store.SqlldrStoreParam;

import com.turk.task.CollectObjInfo;
import com.turk.task.DevInfo;
import com.turk.templet.DBAutoTempletP;
import com.turk.templet.DBAutoTempletP2;
import com.turk.templet.Table;
import com.turk.util.CommonDB;
import com.turk.util.Util;

public class DBAutoParser2 extends Parser
{
  private SqlldrStore sqlldrStore;
  private Map<String, Map<String, String>> mappingfields = null;

  private List<File> clobFiles = new ArrayList<File>();
  private int clobIndex;
  public int parseData(ResultSet rs, DBAutoTempletP2.Templet temP)
    throws Exception
  {
    int recordCount = 0;

    DBAutoTempletP2 temp = (DBAutoTempletP2)this.collectObjInfo.getParseTemplet();
    this.mappingfields = temp.getMappingfields();

    String desTable = temP.getDestTableName();
    String locsql = toSql(desTable, null);
    locsql = locsql + " where 1=2";
    Connection destConn = null;
    PreparedStatement descPs = null;
    ResultSet destRs = null;
    try
    {
      destConn = CommonDB.getConnection();
      descPs = destConn.prepareStatement(locsql);
      destRs = descPs.executeQuery();

      boolean isOracle = this.collectObjInfo.getDBDriver().contains("oracle");

      ResultSetMetaData srcMeta = rs.getMetaData();
      ResultSetMetaData destMeta = destRs.getMetaData();

      Table table = new Table();
      table.setName(temP.getDestTableName());
      table.setId(temP.getId());
      table.setSplitSign(";");

      parseMeta(srcMeta, destMeta, table);

      while (rs.next())
      {
        StringBuilder colVals = new StringBuilder();
        Collection<Table.Column> colection = table.getColumns().values();
        for (Table.Column col : colection)
        {
          int extIndex = col.getExtIndex();
          String colVal = null;
          if (col.getType() != 4)
          {
            colVal = rs.getString(extIndex);
            colVals.append(removeNoise(col.getType(), colVal)).append(";");
          }
          else
          {
            if (isOracle)
            {
              colVal = ConstDef.ClobParse(rs.getClob(extIndex));
            }
            else {
              colVal = rs.getString(extIndex);
            }
            File clob = new File(SystemConfig.getInstance().getCurrentPath(), "clob_" + 
              this.collectObjInfo.getTaskID() + 
              "_" + 
              Util.getDateString_yyyyMMddHHmmssSSS(this.collectObjInfo.getLastCollectTime()) + 
              "_" + this.clobIndex++ + ".clob");
            PrintWriter pw = new PrintWriter(clob);
            pw.print(colVal == null ? "" : colVal);
            pw.flush();
            pw.close();
            colVals.append(clob.getAbsolutePath()).append(";");
            this.clobFiles.add(clob);
          }
        }
        recordCount++;
        distribute(colVals.toString(), temP.getId(), table);
      }
    }
    finally
    {
      commit();
      CommonDB.close(destRs, descPs, destConn);
    }

    return recordCount;
  }

  private String toSql(String tableName, String condition)
  {
    String sql = null;
    if (Util.isNotNull(tableName))
    {
      sql = "select * from " + tableName;
      if (Util.isNotNull(condition))
      {
        condition = ConstDef.ParseFilePathForDB(condition, this.collectObjInfo.getLastCollectTime());
        sql = sql + " where " + condition;
      }
    }
    return sql;
  }

  private void parseMeta(ResultSetMetaData srcMeta, ResultSetMetaData descMeta, Table table) throws Exception
  {
    int srcColCount = srcMeta.getColumnCount();
    int destColCount = descMeta.getColumnCount();
    String tbName = table.getName();
    Map<?, ?> mcMap = null;

    for (int j = 1; j <= destColCount; j++)
    {
      String destColName = descMeta.getColumnName(j);

      if ((destColName.equalsIgnoreCase("DEVICEID")) || 
        (destColName.equalsIgnoreCase("COLLECTTIME")) || 
        (destColName.equalsIgnoreCase("STAMPTIME"))) {
        continue;
      }
      for (int i = 1; i <= srcColCount; i++)
      {
        String srcColName = srcMeta.getColumnName(i);

        if ((this.mappingfields.containsKey(tbName)) && 
          (this.mappingfields.get(tbName) != null) && 
          (((Map<?, ?>)this.mappingfields.get(tbName)).size() > 0))
        {
          mcMap = (Map<?, ?>)this.mappingfields.get(tbName);
          if ((mcMap.containsKey(destColName)) && 
            (((String)mcMap.get(destColName)).equalsIgnoreCase(srcColName)))
          {
            putColumn(destColName, j, i, srcMeta, descMeta, table);
            break;
          }
        }

        if (!srcColName.equalsIgnoreCase(destColName))
          continue;
        putColumn(destColName, j, i, srcMeta, descMeta, table);
        break;
      }
    }
  }

  private void putColumn(String colName, int index, int extIndex, ResultSetMetaData srcMeta, ResultSetMetaData meta, Table table)
    throws SQLException
  {
	  Table tmp11_8 = new Table(); 
     tmp11_8.getClass(); 
     Table.Column column = tmp11_8.new Column();
    column.setIndex(index);
    column.setExtIndex(extIndex);
    column.setName(colName);

    int type = meta.getColumnType(index);
    int typeTmp = 1;

    if (type == 2)
    {
      typeTmp = 1;
      column.setFormat(String.valueOf(meta.getPrecision(index)));
    }
    else if (type == 12)
    {
      typeTmp = 2;
      column.setFormat(String.valueOf(meta.getPrecision(index)));
    }
    else if ((type == 91) || (type == 92) || (type == 93))
    {
      typeTmp = 3;
      column.setFormat("YYYY-MM-DD HH24:MI:SS");
    }
    else if (type == 2005)
    {
      typeTmp = 4;
      column.setFormat("80000");
    }
    else {
      column.setFormat(String.valueOf(meta.getPrecision(index)));
    }
    column.setType(typeTmp);
    table.getColumns().put(Integer.valueOf(index), column);
  }

  private void distribute(String lineData, int templetId, Table tableD)
    throws StoreException
  {
    if (this.sqlldrStore == null)
    {
      this.sqlldrStore = new SqlldrStore(new SqlldrStoreParam(templetId, tableD));
      this.sqlldrStore.setCollectInfo(this.collectObjInfo);
      this.sqlldrStore.setTaskID(this.collectObjInfo.getTaskID());
      this.sqlldrStore.setDataTime(this.collectObjInfo.getLastCollectTime());
      this.sqlldrStore.setDeviceID(this.collectObjInfo.getDevInfo().getDevID());
      this.sqlldrStore.open();
    }
    this.sqlldrStore.write(lineData);
  }

  private void commit()
  {
    if (this.sqlldrStore != null)
    {
      try
      {
        this.sqlldrStore.flush();
        this.sqlldrStore.commit();
        this.sqlldrStore.close();
        this.sqlldrStore = null;

        for (File f : this.clobFiles)
        {
          f.delete();
        }
        this.clobFiles.clear();
      }
      catch (StoreException localStoreException)
      {
      }
    }
  }

  private String removeNoise(int colType, String colVal)
  {
    if (colVal == null) {
      return "";
    }

    if (colType == 3) {
      return colVal.substring(0, 19);
    }

    colVal = colVal.trim().replaceAll(";", " ").replaceAll("\r\n", " ");
    colVal = colVal.replaceAll("\n", " ").replaceAll("\r", " ");

    return colVal;
  }

  public boolean parseData()
    throws Exception
  {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args)
  {
    CollectObjInfo obj = new CollectObjInfo(755123);
    DBAutoTempletP sect = new DBAutoTempletP();
    try
    {
      sect.parseTemp("clt_pm_alt_b10_gprs_parse.xml");
    }
    catch (Exception e1)
    {
      e1.printStackTrace();
    }

    DistributeTemplet dis = new DistributeTemplet();
    DevInfo dev = new DevInfo();
    dev.setDevID(111);
    obj.setDevInfo(dev);
    obj.setParseTemplet(sect);
    obj.setDistributeTemplet(dis);
    obj.setLastCollectTime(new Timestamp(new Date().getTime()));

    DBAutoParser2 xml = new DBAutoParser2();
    xml.collectObjInfo = obj;

    xml.setFileName("D:\\RMFS00032.200");
    try
    {
      xml.parseData();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

@Override
public void Stop() {
	// TODO Auto-generated method stub
	
}
}