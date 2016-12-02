package com.turk.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.turk.db.pojo.User;
import com.turk.db.pojo.UserGroup;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;

public class UserGroupDAO extends AbstractDAO<UserGroup>
{
  private Logger logger = LogMgr.getInstance().getSystemLogger();

  public int add(UserGroup entity)
  {
    String sql = "insert into utl_CONF_USERGROUP values(?,?,?,?)";
    Connection con = DbPool.getConn();
    Statement st = null;
    PreparedStatement ps = null;
    try
    {
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);
      int index = 1;
      ps.setInt(index++, entity.getId());
      ps.setString(index++, entity.getName());
      ps.setString(index++, entity.getIds());
      ps.setString(index++, entity.getNote());
      ps.execute();
      if (con != null)
      {
        con.commit();
      }
      return 1;
    }
    catch (Exception e)
    {
      this.logger.error("插入数据失败：" + sql, e);
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

  public boolean delete(UserGroup entity)
  {
    return delete(entity.getId());
  }

  public boolean delete(int id)
  {
    String sql = "delete utl_CONF_USERGROUP t where t.id=" + id;
    int i = 0;
    try
    {
      i = CommonDB.executeUpdate(sql);
      return i > 0;
    }
    catch (SQLException e)
    {
      this.logger.error("删除记录时异常:" + sql, e);
    }return false;
  }

  public UserGroup getById(int id)
  {
    UserGroup user = null;

    String sql = "select t.* from utl_CONF_USERGROUP t where t.id=" + id;
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        user = new UserGroup();
        user.setId(rs.getInt("ID"));
        user.setName(rs.getString("GROUPNAME"));
        user.setIds(rs.getString("IDS"));
        user.setNote(rs.getString("NOTE"));
      }
      else
      {
        return null;
      }
    }
    catch (Exception e)
    {
      this.logger.error("查询记录时异常：" + sql, e);
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

    return user;
  }

  public UserGroup getByName(String name)
  {
    return null;
  }

  public List<UserGroup> list()
  {
    UserGroup userGroup = null;
    String sql = "select g.id as groupid,g.groupname,g.ids,g.note,u.id userid,u.username,u.password from utl_CONF_USERGROUP g left join utl_CONF_USER u on g.id = u.groupid order by g.id asc";
    List userGroups = new ArrayList();
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        userGroup = new UserGroup();
        userGroup.setId(rs.getInt("groupid"));
        userGroup.setName(rs.getString("GROUPNAME"));
        userGroup.setIds(rs.getString("IDS"));
        userGroup.setNote(rs.getString("NOTE"));

        User user = null;
        int userID = rs.getInt("userid");
        if (userID > 0)
        {
          user = new User();
          user.setId(rs.getInt("userid"));
          user.setGroupID(rs.getInt("groupid"));
          user.setUserName(rs.getString("username"));
        }

        if (userGroups.contains(userGroup))
        {
          if (user == null)
            continue;
          UserGroup oUserGroup = (UserGroup)userGroups.get(userGroups.indexOf(userGroup));
          List users = oUserGroup.getUsers();
          users.add(user);
        }
        else
        {
          if (user != null)
          {
            List users = new ArrayList();
            users.add(user);
            userGroup.setUsers(users);
          }
          userGroups.add(userGroup);
        }
      }

    }
    catch (Exception e)
    {
      this.logger.error("查询记录时异常：" + sql, e);
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

    return userGroups;
  }

  public PageQueryResult<UserGroup> pageQuery(int pageSize, int currentPage)
  {
    return null;
  }

  public List<UserGroup> query(String sql)
  {
    return null;
  }

  public boolean update(UserGroup entity)
  {
    String sql = "update utl_CONF_USERGROUP set ID=?,GROUPNAME=?,IDS=?,NOTE=? where ID='" + 
      entity.getId() + "'";
    Connection con = DbPool.getConn();
    Statement st = null;
    PreparedStatement ps = null;
    try
    {
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);
      int index = 1;
      ps.setInt(index++, entity.getId());
      ps.setString(index++, entity.getName());
      ps.setString(index++, entity.getIds());
      ps.setString(index++, entity.getNote());
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
      this.logger.error("更新数据失败：" + sql, e);
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

  public boolean validate(UserGroup entity)
  {
    boolean bool = true;
    int newId = entity.getId();
    UserGroupDAO dao = new UserGroupDAO();
    List<UserGroup> list = dao.list();
    for (UserGroup g : list)
    {
      if (g.getId() != newId)
        continue;
      bool = false;
    }

    return bool;
  }

  public static void main(String[] args)
  {
  }
}