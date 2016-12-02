package com.turk.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.jsp.jstl.sql.Result;

import com.turk.db.pojo.User;
import com.turk.db.pojo.UserGroup;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.Util;

public class UserDAO extends AbstractDAO<User>
{
  public int add(User entity)
  {
    String sql = "insert into utl_conf_user values(seq_utl_conf_user.nextval,?,?,?)";
    Connection con = DbPool.getConn();
    Statement st = null;
    PreparedStatement ps = null;
    try
    {
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);
      int index = 1;
      ps.setString(index++, entity.getUserName());
      ps.setString(index++, Util.toMD5(entity.getUserPwd()));
      ps.setInt(index++, entity.getGroupID());
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

  public boolean delete(int id)
  {
    String sql = "delete utl_conf_user u where u.id=" + id;
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

  public boolean delete(User entity)
  {
    return delete(entity.getId());
  }

  public List<User> list()
  {
    String sql = "select u.id as userid,u.username,u.password,g.id as groupid,g.groupname,g.ids,g.note from utl_conf_user u left join utl_conf_usergroup g on u.groupid = g.id order by u.id asc";
    List<User> lst = new ArrayList<User>();

    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        User u = new User();
        u.setId(rs.getInt("userid"));
        u.setUserName(rs.getString("USERNAME"));
        u.setUserPwd(rs.getString("PASSWORD"));

        UserGroup group = new UserGroup();
        group.setId(rs.getInt("groupid"));
        group.setIds(rs.getString("ids"));
        group.setName(rs.getString("groupname"));
        group.setNote(rs.getString("note"));
        u.setGroup(group);

        lst.add(u);
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

  public User getById(int id)
  {
    User u = null;

    String sql = "select u.* from utl_conf_user u where u.id=" + id;
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        u = new User();
        u.setId(id);
        u.setUserName(rs.getString("USERNAME"));
        u.setUserPwd(rs.getString("PASSWORD"));
        u.setGroupID(rs.getInt("GROUPID"));
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

    return u;
  }

  public User getByName(String name)
  {
    User u = null;

    String sql = "select u.* from utl_conf_user u where u.USERNAME='" + 
      name + "'";
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        u = new User();
        u.setId(rs.getInt("id"));
        u.setUserName(name);
        u.setUserPwd(rs.getString("PASSWORD"));
        u.setGroupID(rs.getInt("GROUPID"));
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

    return u;
  }

  @SuppressWarnings("unused")
public boolean update(User entity)
  {
    boolean bFlag = false;
    String sql = "update utl_conf_user set GROUPID=%s where id=%s";
    sql = String.format(sql, new Object[] { Integer.valueOf(entity.getGroupID()), Integer.valueOf(entity.getId()) });
    sql = sql.replaceAll("='null'", "=''");
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      int num = st.executeUpdate(sql);
      if (num >= 1)
      {
        bFlag = true;
      }
      else
      {
        bFlag = false;
      }
    }
    catch (Exception e)
    {
      logger.error("修改记录时异常：" + sql, e);
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

    return bFlag;
  }

  public boolean modifyPwd(int userID, String newPwd)
  {
    boolean bFlag = false;
    String sql = "update utl_conf_user set PASSWORD='%s' where id=%s";
    sql = String.format(sql, new Object[] { Util.toMD5(newPwd), Integer.valueOf(userID) });
    Connection con = DbPool.getConn();
    Statement st = null;
    try
    {
      st = con.createStatement();
      bFlag = st.executeUpdate(sql) >= 1;
    }
    catch (Exception e)
    {
      logger.error("密码修改时异常：" + sql, e);
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

  public boolean validate(User entity)
  {
    try
    {
      String sql = "select count(*) as c from utl_conf_user t where t.username='%s'";

      sql = String.format(sql, new Object[] { entity.getUserName() });

      Result r = null;

      r = CommonDB.queryForResult(sql);
      int c = Integer.parseInt(r.getRows()[0].get("c").toString());
      return c == 1;
    }
    catch (Exception e)
    {
      logger.error("验证用户信息时异常", e);
    }return false;
  }

  	public boolean checkAccount(User entity)
  	{
  		try
  		{
  			String sql = "select count(*) as c from utl_conf_user t where t.username='%s' and t.password='%s'";

  			sql = String.format(sql, new Object[] { entity.getUserName(), Util.toMD5(entity.getUserPwd()) });

  			Result r = null;

  			r = CommonDB.queryForResult(sql);
  			int c = Integer.parseInt(r.getRows()[0].get("c").toString());
  			return c == 1;
  		}
  		catch (Exception e)
  		{
  			logger.error("验证用户信息时异常", e);
  		}
  		return false;
  	}

  public int clearAll()
  {
    int count = 0;
    String sql = "delete utl_conf_user";
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