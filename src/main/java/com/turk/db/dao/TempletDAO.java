package com.turk.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.turk.db.pojo.Templet;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.Util;

public class TempletDAO extends AbstractDAO<Templet>
{
  public int add(Templet entity)
  {
    String sql = "insert into utl_conf_templet values(?,?,?,?,?)";
    Connection con = DbPool.getConn();
    Statement st = null;
    PreparedStatement ps = null;
    try
    {
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);
      int index = 1;
      ps.setInt(index++, entity.getTmpID());
      ps.setInt(index++, entity.getTmpType());
      ps.setString(index++, entity.getTmpName());
      ps.setString(index++, entity.getEdition());
      ps.setString(index++, entity.getTempFileName());

      ps.execute();
      if (con != null)
      {
        con.commit();
      }
      return 1;
    }
    catch (Exception e)
    {
      logger.error("插入数据失败：" + sql, e);
      try
      {
        if (con != null)
        {
          con.rollback();
        }
      }
      catch (Exception localException2)
      {
      }
      return 0;
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
      catch (Exception localException4) {
    	  localException4.printStackTrace();
      }
    }
  }

  public boolean delete(Templet entity)
  {
    return delete(entity.getTmpID());
  }

  public boolean delete(int id)
  {
    String sql = "delete utl_conf_templet t where t.tmpid=" + id;
    int i = 0;
    try
    {
      i = CommonDB.executeUpdate(sql);
      return i > 0;
    }
    catch (SQLException e)
    {
      logger.error("删除记录时异常:" + sql, e);
    }return false;
  }

  public Templet getById(int id)
  {
    Templet tem = null;

    String sql = "select t.* from utl_conf_templet t where t.tmpid=" + id;
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        tem = new Templet();
        tem.setTmpID(rs.getInt("TMPID"));
        tem.setTmpType(rs.getInt("TMPTYPE"));
        tem.setTmpName(rs.getString("TMPNAME"));
        tem.setEdition(rs.getString("EDITION"));
        tem.setTempFileName(rs.getString("TEMPFILENAME"));
      }
      else
      {
        return null;
      }
    }
    catch (Exception e)
    {
      logger.error("查询记录时异常：" + sql, e);
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
      catch (Exception localException3)
      {
      }
    }
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
    catch (Exception localException4)
    {
    }

    return tem;
  }

  public List<Templet> list()
  {
    Templet tem = null;

    String sql = "select * from utl_conf_templet";
    List tems = new ArrayList();
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        tem = new Templet();
        tem.setTmpID(rs.getInt("TMPID"));
        tem.setTmpType(rs.getInt("TMPTYPE"));
        tem.setTmpName(rs.getString("TMPNAME"));
        tem.setEdition(rs.getString("EDITION"));
        tem.setTempFileName(rs.getString("TEMPFILENAME"));
        tems.add(tem);
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

    return tems;
  }

  public boolean update(Templet entity)
  {
    String sql = "update utl_conf_templet set TMPID=?,TMPTYPE=?,TMPNAME=?,EDITION=?,TEMPFILENAME=? where tmpid='" + 
      entity.getTmpID() + "'";
    Connection con = DbPool.getConn();
    Statement st = null;
    PreparedStatement ps = null;
    try
    {
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);
      int index = 1;
      ps.setInt(index++, entity.getTmpID());
      ps.setInt(index++, entity.getTmpType());
      ps.setString(index++, entity.getTmpName());
      ps.setString(index++, entity.getEdition());
      ps.setString(index++, entity.getTempFileName());
      int num = ps.executeUpdate();
      if (num < 1)
      {
        if (con != null)
        {
          con.rollback();
        }
        return false;
      }
      if (con != null)
      {
        con.commit();
      }
      return true;
    }
    catch (Exception e)
    {
      logger.error("更新数据失败：" + sql, e);
      e.printStackTrace();
      try
      {
        if (con != null)
        {
          con.rollback();
        }
      }
      catch (Exception localException3)
      {
      }
      return false;
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
      catch (Exception localException5) {
    	  localException5.printStackTrace();
      }
    }
  }

  public boolean exists(Templet entity)
  {
    if (entity == null) {
      return false;
    }

    int tmpID = entity.getTmpID();
    Templet tmp = getById(tmpID);
    if (tmp != null) return true;

    boolean ret = false;

    String tmpFileName = entity.getTempFileName();

    if (Util.isNull(tmpFileName)) {
      return false;
    }
    String sql = "select t.* from utl_conf_templet t where t.tempfilename='" + 
      tmpFileName + "'";
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        ret = true;
      }
    }
    catch (Exception e)
    {
      logger.error("查询记录时异常：" + sql, e);
      ret = true;
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

    return ret;
  }

  public List<Templet> criteriaQuery(Templet temp)
  {
    int id = 0;
    String name = null;
    String des = null;
    int type = 0;

    if (temp != null)
    {
      id = temp.getTmpID();
      name = temp.getTempFileName();
      type = temp.getTmpType();
      des = temp.getTmpName();
    }

    String basicSQL = "select t.* from utl_conf_templet t";
    StringBuffer sql = new StringBuffer(basicSQL);

    List conditions = new ArrayList();
    if (id > 0)
      conditions.add("t.tmpid=" + id);
    if (name != null)
      conditions.add("t.tempfilename like '%" + name + "%'");
    if (type > 0)
      conditions.add("t.tmptype=" + type);
    if (des != null) {
      conditions.add("t.tmpname like '%" + des + "%'");
    }

    if (conditions.size() >= 1)
    {
      sql.append(" where ").append((String)conditions.get(0));
      for (int i = 1; i < conditions.size(); i++)
      {
        sql.append(" and ").append((String)conditions.get(i));
      }
    }

    List tmps = new ArrayList();
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    Templet tTmp = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql.toString());
      while (rs.next())
      {
        tTmp = new Templet();
        tTmp.setTmpID(rs.getInt("TMPID"));
        tTmp.setTmpType(rs.getInt("TMPTYPE"));
        tTmp.setTmpName(rs.getString("TMPNAME"));
        tTmp.setEdition(rs.getString("EDITION"));
        tTmp.setTempFileName(rs.getString("TEMPFILENAME"));
        tmps.add(tTmp);
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

    return tmps;
  }

  public List<String> getMetaData(String tableName)
  {
    Connection connection = DbPool.getConn();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    List list = new ArrayList();
    try
    {
      stmt = connection.prepareStatement("select column_name,data_type,data_length from all_tab_columns where upper(table_name)= ?");
      stmt.setString(1, tableName.toUpperCase());
      rs = stmt.executeQuery();
      while (rs.next())
      {
        list.add(rs.getString(1));
      }
      rs.close();
      stmt.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      logger.error("获取元数据时出现异常.", e);
      try
      {
        if (stmt != null)
        {
          stmt.close();
        }
        if (connection != null)
        {
          connection.close();
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
        if (stmt != null)
        {
          stmt.close();
        }
        if (connection != null)
        {
          connection.close();
        }
      }
      catch (Exception localException2)
      {
      }
    }
    return list;
  }

  public static void main(String[] args)
  {
    Templet tm = new Templet();
    tm.setTmpID(800);
    tm.setTmpType(22);
    tm.setTmpName("utl_1.0_模板");
    tm.setEdition("1.0");
    tm.setTempFileName("utlTemplet.xml");

    TempletDAO dao = new TempletDAO();

    Templet t = dao.getById(50);
    System.out.println(t.getTempFileName());

    dao.delete(tm);
  }
}