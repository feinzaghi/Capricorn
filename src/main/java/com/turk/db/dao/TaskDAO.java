package com.turk.db.dao;

import com.turk.Config.ConstDef;

import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;

import oracle.sql.CLOB;

import org.apache.log4j.Logger;

import com.turk.db.pojo.CollectPeriod;
import com.turk.db.pojo.CollectType;
import com.turk.db.pojo.Task;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class TaskDAO extends AbstractDAO<Task>
{
  private Logger logger = LogMgr.getInstance().getSystemLogger();
  
  private String handNullNum(int num)
  {
    if (num == -1)
    {
      return "null";
    }

    return String.valueOf(num);
  }

  public int add(Task entity)
  {
    String sql = "insert into utl_conf_task (task_id, task_describe, dev_id, dev_port, proxy_dev_id, proxy_dev_port, collect_type, collect_period, collecttimeout, collect_time, collect_path, shell_timeout, parse_tmpid, distrbute_tmpid, suc_data_time, suc_data_pos, isused, isupdate, maxclttime, shell_cmd_prepare, shell_cmd_finish, collect_timepos, dbdriver, dburl, threadsleeptime, blockedtime, collector_name, paramrecord, group_id, end_data_time, parserid, distributorid, redo_time_offset) values (%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,empty_clob(),%s,%s,%s,to_date('%s','yyyy-mm-dd hh24:mi:ss'),%s,%s,%s,%s,'%s','%s',%s,'%s','%s',%s,%s,'%s',%s,%s,%s,%s,%s,%s)";

    sql = String.format(sql, new Object[] { Integer.valueOf(entity.getTaskId()), entity.getTaskDescribe(), Integer.valueOf(entity.getDevId()), handNullNum(entity.getDevPort()), handNullNum(entity.getProxyDevId()), handNullNum(entity.getProxyDevPort()), Integer.valueOf(entity.getCollectType().getValue()), Integer.valueOf(entity.getCollectPeriod().getValue()), handNullNum(entity.getCollectTimeout()), handNullNum(entity.getCollectTime()), handNullNum(entity.getShellTimeout()), handNullNum(entity.getParseTmpId()), handNullNum(entity.getDistributeTmpId()), Util.getDateString(entity.getSucDataTime()), handNullNum(entity.getSucDataPos()), entity.getIsUsed() == -1 ? "0" : Integer.valueOf(entity.getIsUsed()), entity.getIsUpdate() == -1 ? "0" : Integer.valueOf(entity.getIsUpdate()), handNullNum(entity.getMaxCltTime()), entity.getShellCmdPrepare(), entity.getShellCmdFinish(), handNullNum(entity.getCollectTimepos()), entity.getDbDriver(), entity.getDbUrl(), handNullNum(entity.getThreadSleepTime()), handNullNum(entity.getBlockTime()), entity.getCollectorName(), handNullNum(entity.getParamRecord()), handNullNum(entity.getGroupId()), "to_date('" + 
      Util.getDateString(entity.getEndDataTime()) + "','yyyy-mm-dd hh24:mi:ss')", handNullNum(entity.getParserId()), handNullNum(entity.getDistributorId()), handNullNum(entity.getRedoTimeOffset()) });
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      con.setAutoCommit(false);
      st = con.createStatement();
      st.execute(sql);
      rs = st.executeQuery("select t.collect_path from utl_conf_task t where t.task_id=" + 
        entity.getTaskId() + " for update");
      rs.next();
      CLOB clob = (CLOB)rs.getClob(1);
      Writer out = clob.getCharacterOutputStream();
      out.write(entity.getCollectPath());
      out.flush();
      out.close();
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
      catch (Exception localException4) {
    	  localException4.printStackTrace();
      }
    }
  }

  public boolean delete(Task entity)
  {
    return delete(entity.getTaskId());
  }

  public boolean delete(int id)
  {
    String sql = "delete utl_conf_task t where t.task_id=" + id;
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

  private int getIntFromResultSet(ResultSet rs, String name)
    throws Exception
  {
    String str = rs.getString(name);
    return Util.isNull(str) ? -1 : rs.getInt(name);
  }

  public Task getById(int id)
  {
    Task t = null;

    String sql = "select t.* from utl_conf_task t where t.task_id=" + id;
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      if (rs.next())
      {
        t = new Task(rs.getInt("task_id"));
        t.setTaskDescribe(rs.getString("task_describe"));
        t.setDevId(getIntFromResultSet(rs, "dev_id"));
        t.setDevPort(getIntFromResultSet(rs, "dev_port"));
        t.setProxyDevId(getIntFromResultSet(rs, "proxy_dev_id"));
        t.setProxyDevPort(getIntFromResultSet(rs, "proxy_dev_port"));
        t.setCollectType(CollectType.create(rs.getInt("collect_type")));
        t.setCollectPeriod(CollectPeriod.create(rs.getInt("collect_period")));
        t.setCollectTimeout(getIntFromResultSet(rs, "collecttimeout"));
        t.setCollectTime(getIntFromResultSet(rs, "collect_time"));
        t.setCollectPath(ConstDef.ClobParse(rs.getClob("collect_path")));
        t.setShellTimeout(getIntFromResultSet(rs, "shell_timeout"));
        t.setParseTmpId(getIntFromResultSet(rs, "parse_tmpid"));
        t.setDistributeTmpId(getIntFromResultSet(rs, "distrbute_tmpid"));
        t.setSucDataTime(rs.getTimestamp("suc_data_time"));
        t.setSucDataPos(getIntFromResultSet(rs, "suc_data_pos"));
        t.setIsUsed(getIntFromResultSet(rs, "isused"));
        t.setIsUpdate(getIntFromResultSet(rs, "isupdate"));
        t.setMaxCltTime(getIntFromResultSet(rs, "maxclttime"));
        t.setShellCmdPrepare(rs.getString("shell_cmd_prepare"));
        t.setShellCmdFinish(rs.getString("shell_cmd_finish"));
        t.setCollectTimepos(rs.getInt("collect_timepos"));
        t.setDbDriver(rs.getString("dbdriver"));
        t.setDbUrl(rs.getString("dburl"));
        t.setThreadSleepTime(rs.getInt("threadsleeptime"));
        t.setBlockTime(getIntFromResultSet(rs, "blockedtime"));
        t.setCollectorName(rs.getString("collector_name"));
        t.setParamRecord(getIntFromResultSet(rs, "paramrecord"));
        t.setGroupId(getIntFromResultSet(rs, "group_id"));
        t.setEndDataTime(rs.getTimestamp("end_data_time"));
        t.setParserId(getIntFromResultSet(rs, "parserid"));
        t.setDistributorId(getIntFromResultSet(rs, "distributorid"));
        t.setRedoTimeOffset(getIntFromResultSet(rs, "redo_time_offset"));
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

    return t;
  }

  public Task getByName(String name)
  {
    throw new UnsupportedOperationException();
  }

  public List<Task> list()
  {
    String sql = "select * from utl_conf_task";
    List tasks = new ArrayList();

    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        Task t = new Task(rs.getInt("task_id"));
        t.setTaskDescribe(rs.getString("task_describe"));
        t.setDevId(rs.getInt("dev_id"));
        t.setDevPort(rs.getInt("dev_port"));
        t.setProxyDevId(rs.getInt("proxy_dev_id"));
        t.setProxyDevPort(rs.getInt("proxy_dev_port"));
        t.setCollectType(CollectType.create(rs.getInt("collect_type")));
        t.setCollectPeriod(CollectPeriod.create(rs.getInt("collect_period")));
        t.setCollectTimeout(rs.getInt("collecttimeout"));
        t.setCollectTime(rs.getInt("collect_time"));
        t.setCollectPath(ConstDef.ClobParse(rs.getClob("collect_path")));
        t.setShellTimeout(rs.getInt("shell_timeout"));
        t.setParseTmpId(rs.getInt("parse_tmpid"));
        t.setDistributeTmpId(rs.getInt("distrbute_tmpid"));
        t.setSucDataTime(rs.getTimestamp("suc_data_time"));
        t.setSucDataPos(rs.getInt("suc_data_pos"));
        t.setIsUsed(rs.getInt("isused"));
        t.setIsUpdate(rs.getInt("isupdate"));
        t.setMaxCltTime(rs.getInt("maxclttime"));
        t.setShellCmdPrepare(rs.getString("shell_cmd_prepare"));
        t.setShellCmdFinish(rs.getString("shell_cmd_finish"));
        t.setCollectTimepos(rs.getInt("collect_timepos"));
        t.setDbDriver(rs.getString("dbdriver"));
        t.setDbUrl(rs.getString("dburl"));
        t.setThreadSleepTime(rs.getInt("threadsleeptime"));
        t.setBlockTime(rs.getInt("blockedtime"));
        t.setCollectorName(rs.getString("collector_name"));
        t.setParamRecord(rs.getInt("paramrecord"));
        t.setGroupId(rs.getInt("group_id"));
        t.setEndDataTime(rs.getTimestamp("end_data_time"));
        t.setParserId(rs.getInt("parserid"));
        t.setDistributorId(rs.getInt("distributorid"));
        t.setRedoTimeOffset(rs.getInt("redo_time_offset"));
        tasks.add(t);
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
    return tasks;
  }

  public PageQueryResult<Task> pageQuery(int pageSize, int currentPage, Map<String, String> condition)
  {
    return null;
  }

  public PageQueryResult<Task> pageQuery(int pageSize, int currentPage)
  {
    return advQuery(null, pageSize, currentPage);
  }

  public List<Task> query(String sql)
  {
    throw new UnsupportedOperationException();
  }

  public boolean update(Task entity)
  {
    String sql = "update utl_conf_task set task_describe='%s',dev_id=%s,dev_port=%s,proxy_dev_id=%s,proxy_dev_port=%s,collect_type=%s,collect_period=%s,collecttimeout=%s,collect_time=%s,collect_path=empty_clob(),shell_timeout=%s,parse_tmpid=%s,distrbute_tmpid=%s,suc_data_time=to_date('%s','yyyy-mm-dd hh24:mi:ss'),suc_data_pos=%s,isused=%s,isupdate=%s,maxclttime=%s,shell_cmd_prepare='%s',shell_cmd_finish='%s',collect_timepos=%s,dbdriver='%s',dburl='%s',threadsleeptime=%s,blockedtime=%s,collector_name='%s',paramrecord=%s,group_id=%s,end_data_time=%s,parserid=%s,distributorid=%s,redo_time_offset=%s where task_id=%s";

    sql = String.format(sql, new Object[] { entity.getTaskDescribe(), Integer.valueOf(entity.getDevId()), Integer.valueOf(entity.getDevPort()), Integer.valueOf(entity.getProxyDevId()), Integer.valueOf(entity.getProxyDevPort()), Integer.valueOf(entity.getCollectType().getValue()), Integer.valueOf(entity.getCollectPeriod().getValue()), Integer.valueOf(entity.getCollectTimeout()), Integer.valueOf(entity.getCollectTime()), Integer.valueOf(entity.getShellTimeout()), Integer.valueOf(entity.getParseTmpId()), Integer.valueOf(entity.getDistributeTmpId()), Util.getDateString(entity.getSucDataTime()), Integer.valueOf(entity.getSucDataPos()), Integer.valueOf(entity.getIsUsed()), Integer.valueOf(entity.getIsUpdate()), Integer.valueOf(entity.getMaxCltTime()), entity.getShellCmdPrepare(), entity.getShellCmdFinish(), Integer.valueOf(entity.getCollectTimepos()), entity.getDbDriver(), entity.getDbUrl(), Integer.valueOf(entity.getThreadSleepTime()), Integer.valueOf(entity.getBlockTime()), entity.getCollectorName(), Integer.valueOf(entity.getParamRecord()), Integer.valueOf(entity.getGroupId()), "to_date('" + 
      Util.getDateString(entity.getEndDataTime()) + "','yyyy-mm-dd hh24:mi:ss')", Integer.valueOf(entity.getParserId()), Integer.valueOf(entity.getDistributorId()), Integer.valueOf(entity.getRedoTimeOffset()), Integer.valueOf(entity.getTaskId()) });
    sql = sql.replaceAll("='null'", "=''");
    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      con.setAutoCommit(false);
      st = con.createStatement();
      if (st.executeUpdate(sql) < 1)
      {
        if (con != null)
        {
          con.rollback();
        }
        return false;
      }
      rs = st.executeQuery("select t.collect_path from utl_conf_task t where t.task_id=" + 
        entity.getTaskId() + " for update");
      rs.next();
      CLOB clob = (CLOB)rs.getClob(1);
      Writer out = clob.getCharacterOutputStream();
      out.write(entity.getCollectPath());
      out.flush();
      out.close();
      if (con != null)
      {
        con.commit();
      }
      return true;
    }
    catch (Exception e)
    {
      this.logger.error("修改记录时异常：" + sql, e);
      if (con != null)
      {
        try
        {
          con.rollback();
        }
        catch (SQLException localSQLException)
        {
        }
      }
      return false;
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
      catch (Exception localException4) {
    	  localException4.printStackTrace();
      }
    }
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
      this.logger.error("获取元数据时出现异常.", e);
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

  public PageQueryResult<Task> advQuery(Task t, int pageSize, int currentPage)
  {
    int start = pageSize * currentPage - pageSize + 1;
    int end = pageSize * currentPage;
    int recordCount = 0;
    String sql = "";
    if (t == null)
    {
      sql = "with partdata as (select rownum rowno,t.* from utl_conf_task t ) select * from partdata where rowno between __start and __end";
    }
    else
    {
      StringBuilder b = new StringBuilder();
      b.append("with partdata as (select rownum rowno,t.* from utl_conf_task t where 1<>0");
      if (t.getTaskId() != -1)
        b.append(" and t.task_id=").append(t.getTaskId());
      if (Util.isNotNull(t.getTaskDescribe()))
        b.append(" and t.task_describe like '%").append(t.getTaskDescribe()).append("%'");
      if (t.getDevId() != -1)
        b.append(" and t.dev_id=").append(t.getDevId());
      if (t.getDevPort() != -1)
        b.append(" and t.dev_port=").append(t.getDevPort());
      if (t.getProxyDevId() != -1)
        b.append(" and t.proxy_dev_id=").append(t.getProxyDevId());
      if (t.getProxyDevPort() != -1)
        b.append(" and t.proxy_dev_port=").append(t.getProxyDevPort());
      if (t.getCollectType().getValue() != -1)
        b.append(" and t.collect_type=").append(t.getCollectType().getValue());
      if (t.getCollectPeriod().getValue() != -1)
        b.append(" and t.collect_period=").append(t.getCollectPeriod().getValue());
      if (t.getCollectTimeout() != -1)
        b.append(" and t.collecttimeout=").append(t.getCollectTimeout());
      if (t.getCollectTime() != -1)
        b.append(" and t.collect_time=").append(t.getCollectTime());
      if (Util.isNotNull(t.getCollectPath()))
        b.append(" and t.collect_path like '%").append(t.getCollectPath()).append("%'");
      if (t.getShellTimeout() != -1)
        b.append(" and t.shell_timeout=").append(t.getShellTimeout());
      if (t.getParseTmpId() != -1)
        b.append(" and t.parse_tmpid=").append(t.getParseTmpId());
      if (t.getDistributeTmpId() != -1)
        b.append(" and t.distrbute_tmpid=").append(t.getDistributeTmpId());
      if (t.getSucDataTime() != null)
        b.append(" and t.suc_data_time=to_data('").append(Util.getDateString(t.getSucDataTime())).append("','yyyy-mm-dd hh24:mi:ss')");
      if (t.getSucDataPos() != -1)
        b.append(" and t.suc_data_pos=").append(t.getSucDataPos());
      if (t.getIsUsed() != -1)
        b.append(" and t.isused=").append(t.getIsUsed());
      if (t.getIsUpdate() != -1)
        b.append(" and t.isupdate=").append(t.getIsUpdate());
      if (t.getMaxCltTime() != -1)
        b.append(" and t.maxclttime=").append(t.getMaxCltTime());
      if (Util.isNotNull(t.getShellCmdPrepare()))
        b.append(" and t.shell_cmd_finish like '%").append(t.getShellCmdPrepare()).append("%'");
      if (Util.isNotNull(t.getShellCmdFinish()))
        b.append(" and t.shell_cmd_finish like '%").append(t.getShellCmdFinish()).append("%'");
      if (t.getCollectTimepos() != -1)
        b.append(" and t.collect_timepos=").append(t.getCollectTimepos());
      if (Util.isNotNull(t.getDbDriver()))
        b.append(" and t.dbdriver like '%").append(t.getDbDriver()).append("%'");
      if (Util.isNotNull(t.getDbUrl()))
        b.append(" and t.dburl like '%").append(t.getDbUrl()).append("%'");
      if (t.getThreadSleepTime() != -1)
        b.append(" and t.threadsleeptime=").append(t.getThreadSleepTime());
      if (t.getBlockTime() != -1)
        b.append(" and t.blockedtime=").append(t.getBlockTime());
      if (Util.isNotNull(t.getCollectorName()))
        b.append(" and t.collector_name like '%").append(t.getCollectorName()).append("%'");
      if (t.getParamRecord() != -1)
        b.append(" and t.paramrecord=").append(t.getParamRecord());
      if (t.getGroupId() != -1)
        b.append(" and t.group_id=").append(t.getGroupId());
      if (t.getEndDataTime() != null)
        b.append(" and t.end_data_time=to_data('").append(Util.getDateString(t.getEndDataTime())).append("','yyyy-mm-dd hh24:mi:ss')");
      if (t.getParserId() != -1)
        b.append(" and t.parserid=").append(t.getParserId());
      if (t.getDistributorId() != -1)
        b.append(" and t.distributorid=").append(t.getDistributorId());
      if (t.getRedoTimeOffset() != -1)
        b.append("and t.redo_time_offset=").append(t.getRedoTimeOffset());
      b.append(") select * from partdata where rowno between __start and __end");
      sql = b.toString();
    }
    sql = sql.replace("__start", String.valueOf(start));
    sql = sql.replace("__end", String.valueOf(end));
    List tasks = new ArrayList();

    Connection con = DbPool.getConn();
    Statement st = null;
    ResultSet rs = null;
    try
    {
      st = con.createStatement();
      rs = st.executeQuery(sql);
      while (rs.next())
      {
        Task task = new Task(rs.getInt("task_id"));
        task.setTaskDescribe(rs.getString("task_describe"));
        task.setDevId(rs.getInt("dev_id"));
        task.setDevPort(rs.getInt("dev_port"));
        task.setProxyDevId(rs.getInt("proxy_dev_id"));
        task.setProxyDevPort(rs.getInt("proxy_dev_port"));
        task.setCollectType(CollectType.create(rs.getInt("collect_type")));
        task.setCollectPeriod(CollectPeriod.create(rs.getInt("collect_period")));
        task.setCollectTimeout(rs.getInt("collecttimeout"));
        task.setCollectTime(rs.getInt("collect_time"));
        task.setCollectPath(ConstDef.ClobParse(rs.getClob("collect_path")));
        task.setShellTimeout(rs.getInt("shell_timeout"));
        task.setParseTmpId(rs.getInt("parse_tmpid"));
        task.setDistributeTmpId(rs.getInt("distrbute_tmpid"));
        task.setSucDataTime(rs.getTimestamp("suc_data_time"));
        task.setSucDataPos(rs.getInt("suc_data_pos"));
        task.setIsUsed(rs.getInt("isused"));
        task.setIsUpdate(rs.getInt("isupdate"));
        task.setMaxCltTime(rs.getInt("maxclttime"));
        task.setShellCmdPrepare(rs.getString("shell_cmd_prepare"));
        task.setShellCmdFinish(rs.getString("shell_cmd_finish"));
        task.setCollectTimepos(rs.getInt("collect_timepos"));
        task.setDbDriver(rs.getString("dbdriver"));
        task.setDbUrl(rs.getString("dburl"));
        task.setThreadSleepTime(rs.getInt("threadsleeptime"));
        task.setBlockTime(rs.getInt("blockedtime"));
        task.setCollectorName(rs.getString("collector_name"));
        task.setParamRecord(rs.getInt("paramrecord"));
        task.setGroupId(rs.getInt("group_id"));
        task.setEndDataTime(rs.getTimestamp("end_data_time"));
        task.setParserId(rs.getInt("parserid"));
        task.setDistributorId(rs.getInt("distributorid"));
        task.setRedoTimeOffset(rs.getInt("redo_time_offset"));
        tasks.add(task);
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
    recordCount = getCount(sql);
    int pageCount = recordCount % pageSize == 0 ? recordCount / pageSize : recordCount / 
      pageSize + 1;
    PageQueryResult pageQueryResult = new PageQueryResult(pageSize, currentPage, pageCount, tasks);
    return pageQueryResult;
  }

  private int getCount(String sql)
  {
    String s = sql.replace("with partdata as (", "");
    s = s.substring(0, s.indexOf(") select"));
    s = s.replace(s.substring(7, s.indexOf(" from")), "count(*) as c");

    Result rs = null;
    try
    {
      rs = CommonDB.queryForResult(s);
    }
    catch (Exception e)
    {
      this.logger.error("查询记录数时异常", e);
    }

    int c = Integer.parseInt(rs.getRows()[0].get("c").toString());

    return c;
  }

  public boolean validate(Task entity)
  {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args)
  {
    System.out.println(new TaskDAO().getCount("with partdata as (select rownum rowno,t.* from utl_conf_task t ) select * from partdata where rowno between 1 and 15"));
  }
}