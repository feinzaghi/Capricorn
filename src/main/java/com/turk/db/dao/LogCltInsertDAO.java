package com.turk.db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.turk.db.pojo.LogCltInsert;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;

public class LogCltInsertDAO extends AbstractDAO<LogCltInsert>
{
  public int delete(byte calValue)
  {
    String sql = "delete LOG_CLT_INSERT t where t.IS_CAL=" + calValue;
    int i = 0;
    try
    {
      i = CommonDB.executeUpdate(sql);
    }
    catch (SQLException e)
    {
      logger.error("ɾ����LOG_CLT_INSERT�м�¼ʱ�쳣:" + sql, e);
    }

    return i;
  }

  public boolean delete(LogCltInsert entity)
  {
    return super.delete(entity);
  }

  public List<LogCltInsert> list()
  {
    String sql = "select * from LOG_CLT_INSERT";
    List<LogCltInsert> lst = new ArrayList<LogCltInsert>();

    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        LogCltInsert o = new LogCltInsert();
        o.setCalFlag(rs.getByte("IS_CAL"));
        o.setCount(rs.getInt("INSERT_COUNTNUM"));
        o.setOmcID(rs.getInt("OMCID"));
        o.setStampTime(new Date(rs.getTimestamp("STAMPTIME").getTime()));
        o.setTbName(rs.getString("CLT_TBNAME"));
        o.setVSysDate(new Date(rs.getTimestamp("VSYSDATE").getTime()));
        o.setTaskID(rs.getInt("TASKID"));

        lst.add(o);
      }
    }
    catch (Exception e)
    {
      logger.error("��ѯ��¼ʱ�쳣��" + sql, e);
      try
      {
        if (rs != null)
        {
          rs.close();
        }
        if (st != null)
        {
          st.close();
        }
        if (con != null)
        {
          con.close();
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
        if (rs != null)
        {
          rs.close();
        }
        if (st != null)
        {
          st.close();
        }
        if (con != null)
        {
          con.close();
        }
      }
      catch (Exception localException2)
      {
      }
    }
    return lst;
  }

  public static void main(String[] args)
  {
  }
}