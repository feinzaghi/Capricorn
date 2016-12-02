package com.turk.db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.turk.db.pojo.Vendor;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;

public class VendorDAO extends AbstractDAO<Vendor>
{
  public boolean delete(int id)
  {
    String sql = "delete utl_conf_vendor v where v.id=" + id;
    int i = 0;
    try
    {
      i = CommonDB.executeUpdate(sql);
    }
    catch (SQLException e)
    {
      logger.error("删除记录时异常:" + sql, e);
    }

    return i > 0;
  }

  public boolean delete(Vendor vendor)
  {
    return delete(vendor.getId());
  }

  public List<Vendor> list()
  {
    String sql = "select * from utl_conf_vendor";
    List<Vendor> lst = new ArrayList<Vendor>();

    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        Vendor v = new Vendor();
        v.setId(rs.getInt("ID"));
        v.setNameCH(rs.getString("VENDORNAME_CH"));
        v.setNameEN(rs.getString("VENDORNAME_EN"));

        lst.add(v);
      }
    }
    catch (Exception e)
    {
      logger.error("查询记录时异常：" + sql, e);
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

  public Vendor getById(int id)
  {
    Vendor v = null;

    String sql = "select t.* from utl_conf_vendor v where v.id=" + id;
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        v = new Vendor();
        v.setId(id);
        v.setNameCH(rs.getString("VENDORNAME_CH"));
        v.setNameEN(rs.getString("VENDORNAME_EN"));
      }
    }
    catch (Exception e)
    {
      logger.error("查询记录时异常：" + sql, e);
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

    return v;
  }

  public boolean update(Vendor vendor)
  {
    boolean bFlag = false;
    String sql = "update utl_conf_vendor set VENDORNAME_CH='%s',VENDORNAME_EN='%s' where id=%s";
    sql = String.format(sql, new Object[] { vendor.getNameCH(), vendor.getNameEN(), Integer.valueOf(vendor.getId()) });
    sql = sql.replaceAll("='null'", "=''");
    Connection con = DbPool.getConn();
    Statement st = null;
    try
    {
      st = con.createStatement();
      bFlag = st.executeUpdate(sql) >= 1;
    }
    catch (Exception e)
    {
      logger.error("修改记录时异常：" + sql, e);
      try
      {
       
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

    return bFlag;
  }

  public int clearAll()
  {
    int count = 0;
    String sql = "delete utl_conf_vendor";
    try
    {
      count = CommonDB.executeUpdate(sql);
    }
    catch (SQLException e)
    {
      logger.error("删除记录时异常:" + sql, e);
    }
    return count;
  }
}