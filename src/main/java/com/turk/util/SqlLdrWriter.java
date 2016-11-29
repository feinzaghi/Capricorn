package com.turk.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

public class SqlLdrWriter
{
  private String fileName;
  private int backlogCount = 100;

  private int count = 0;
  private String charset;
  private String tableName;
  private List<ColumnType> columns;
  private FileWriter fileWriter;

  public SqlLdrWriter(String fileName)
  {
    this.fileName = fileName;
  }

  public SqlLdrWriter(String fileName, int backlogCount)
  {
    this.fileName = fileName;
    this.backlogCount = backlogCount;
  }

  public void write(String data, boolean immediat)
    throws Exception
  {
    if (this.fileWriter == null)
    {
      open();
    }

    this.fileWriter.write(data + "\r\n");
    if (immediat)
    {
      this.fileWriter.flush();
    }
    else if (this.count % this.backlogCount == 0)
    {
      this.fileWriter.flush();
    }

    this.count += 1;
  }

  public String getFileName()
  {
    return this.fileName;
  }

  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }

  public int getBacklogCount()
  {
    return this.backlogCount;
  }

  public void setBacklogCount(int backlogCount)
  {
    this.backlogCount = backlogCount;
  }

  public String getCharset()
  {
    return this.charset;
  }

  public void setCharset(String charset)
  {
    this.charset = charset;
  }

  public String getTableName()
  {
    return this.tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public List<ColumnType> getColumns()
  {
    return this.columns;
  }

  public void setColumns(List<ColumnType> columns)
  {
    this.columns = columns;
  }

  public void dispose()
  {
    try
    {
      this.fileWriter.flush();
      this.fileWriter.close();
      this.fileWriter = null;
      this.fileName = null;
      this.count = 0;
    }
    catch (Exception localException)
    {
    }
  }

  public void commit()
    throws Exception
  {
    try
    {
      this.fileWriter.flush();
    }
    catch (Exception localException)
    {
    }

    createCltFile();
  }

  public void writeHead(String sign) throws Exception
  {
    int len = this.columns.size();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len - 1; i++)
    {
      ColumnType pe = (ColumnType)this.columns.get(i);
      sb.append(pe.getColumnName()).append(sign);
    }
    ColumnType pe = (ColumnType)this.columns.get(len - 1);
    sb.append(pe.getColumnName());

    write(sb.toString(), true);
  }

  private void open() throws Exception
  {
    this.fileWriter = new FileWriter(this.fileName);
  }

  private void createCltFile()
    throws Exception
  {
    String name = this.fileName.substring(0, this.fileName.lastIndexOf(".") + 1) + 
      "clt";
    BufferedWriter bw = new BufferedWriter(new FileWriter(name, false));
    String columnName;
    if (Util.isOracle())
    {
      bw.write("load data\r\n");

      if (Util.isNotNull(this.charset))
        bw.write("CHARACTERSET " + this.charset + " \r\n");
      else {
        bw.write("CHARACTERSET AL32UTF8 \r\n");
      }
      bw.write("infile '" + this.fileName + "'\r\n");
      bw.write("append into table " + this.tableName + " \r\n");
      bw.write("FIELDS TERMINATED BY \";\"\r\n");
      bw.write("TRAILING NULLCOLS\r\n");
      bw.write("(");

      StringBuilder sb = new StringBuilder();
      for (ColumnType ct : this.columns)
      {
        columnName = ct.getColumnName();
        String type = ct.getType();
        if (type.equalsIgnoreCase("date"))
        {
          sb.append(columnName + " \"to_date(:" + columnName + ",'" + 
            ct.getFormat() + "')\"").append(",");
        }
        else if (type.equalsIgnoreCase("lob"))
        {
          sb.append(columnName + " " + ct.getFormat() + ",");
        }
        else
        {
          sb.append(columnName + ",");
        }
      }

      String str = sb.toString();
      bw.write(str.substring(0, str.length() - 1));

      bw.write(")");
      bw.write("\r\n");
    }
    else if ((Util.isSybase()) || (Util.isSqlServer()))
    {
      bw.write("10.0\r\n");
      int size = this.columns.size();
      bw.write(String.valueOf(size) + "\r\n");

      int i = 1;
      for (ColumnType ct : this.columns)
      {
        String colName = ct.getColumnName();

        bw.write(i + "\tSYBCHAR\t0\t128\t");
        if (i < size)
        {
          bw.write("\";\"");
        }
        else
        {
          bw.write("\"\n\"");
        }

        bw.write("\t" + i + "\t" + colName + "\r\n");

        i++;
      }
    }

    bw.close();
  }
}