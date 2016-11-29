package com.turk.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;
import javax.servlet.jsp.jstl.sql.ResultSupport;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class GPCommonDB {
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	public static Map<Integer, String> GetTableColumns(String strTableName)
	{
		String strDriver = SystemConfig.getInstance().getDbDriver();
		if (strDriver.contains("oracle")) {
			return null;
		}
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try
		{
			conn = getConnection();
			String strSQL = "select t1.name as COLUMN_NAME,t1.colid as COLUMN_ID from sysobjects t,syscolumns t1 where t.id=t1.id and t.name='" + 
			strTableName + "'";

			pstmt = conn.prepareStatement(strSQL);
			rs = pstmt.executeQuery();

			Map<Integer, String> columns = new HashMap<Integer, String>();
			while (rs.next())
			{
				columns.put(Integer.valueOf(rs.getInt("COLUMN_ID")), rs.getString("COLUMN_NAME"));
			}
			rs.close();
			pstmt.close();

			Map<Integer, String> localMap1 = columns;
			return localMap1;
		}
		catch (Exception e)
		{
			log.error("CommonDB: GetTableColumns", e);
		}
		finally
		{
			try
			{
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			}
			catch (Exception localException3)
			{
			}
		}
		return null;
	}

	public String toString()
	{
		return "CommonDB";
	}

  /*public static boolean isReAdoptObj(CollectObjInfo taskInfo)
  {
    boolean flag = false;
    if ((taskInfo != null) && ((taskInfo instanceof RegatherObjInfo)))
    {
      flag = true;
    }
    return flag;
  }*/

  	public static void closeDbConnection()
  	{
  		DbPool.close();
  	}

  	/**
  	 * 获取连接
  	 * @param OracleDriver
  	 * @param OracleUrl
  	 * @param OracleUser
  	 * @param OraclePassword
  	 * @return
  	 */
	public static Connection getConnection(String OracleDriver, String OracleUrl, String OracleUser, String OraclePassword)
	{
		Connection conn = null;
		try
		{
			Class.forName(OracleDriver);

			conn = DriverManager.getConnection(OracleUrl, OracleUser, OraclePassword);
		}
		catch (Exception ex)
		{
			log.error("获取连接失败,原因:", ex);
		}

		return conn;
	}

 
	/**
	 * 获取数据库连接
	 * @return
	 */
	public static Connection getConnection()
	{
		return GPDBPool.getConn();
	}


	/**
	 * Insert/Update
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public static int executeUpdate(String sql) throws SQLException
	{
		int count = -1;

		Connection con = null;
		PreparedStatement ps = null;
		try
		{
			con = GPDBPool.getConn();
			ps = con.prepareStatement(sql);
			count = ps.executeUpdate();
		}	
		finally
		{
			close(null, ps, con);
		}

		return count;
	}

	public static Result queryForResult(String sql)
		throws Exception
    {
		Result result = null;
		ResultSet resultSet = null;
		Connection connection = null;
    	PreparedStatement preparedStatement = null;
		try
		{
			connection = GPDBPool.getConn();
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			result = ResultSupport.toResult(resultSet);
			
		}
		finally
		{
			close(resultSet, preparedStatement, connection);
		}
		return result;
    }


	public static int[] executeBatch(List<String> sqlList)
    	throws SQLException
    {
		int[] result = (int[])null;
		Connection con = null;
		Statement stm = null;
		con = DbPool.getConn();
		//String curr = "";
		if (con == null)
		{	
			log.error("批量提交获取数据库连接失败！");
			return result;
		}
		try
		{
			if ((sqlList != null) && (!sqlList.isEmpty()))
			{
				con.setAutoCommit(false);
				stm = con.createStatement();

				for (String sql : sqlList)
				{
					//curr = sql;
					stm.addBatch(sql);
				}
				result = stm.executeBatch();
				con.commit();
			}

		}
		finally
		{
			close(null, stm, con);
		}
		return result;
    }


  	/**
  	 * 关闭数据连接
  	 */
	public static void close()
	{
		DbPool.close();
	}
		  
	/**
	 * 关闭数据库连接以及打开的对象
	 * @param rs
	 * @param stm
	 * @param conn
	 */
	public static void close(ResultSet rs, Statement stm, Connection conn)
	{
		if (rs != null)
		{
			try
			{
				rs.close();
			}
			catch (Exception localException)
      		{
      		}
		}
		if (stm != null)
		{
			try
			{
				stm.close();
			}
			catch (Exception localException1)
			{
			}
		}
		if (conn != null)
		{
			try
			{
				conn.close();
			}
			catch (Exception localException2)
			{
			}
		}
	}

	public static void main(String[] args)
		throws Exception
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			//log.debug("Starting getConnection...");
			conn = GPCommonDB.getConnection();
			//log.debug("GetConnection done...");
			if (conn == null)
			{
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			//获取预定义分组
			strSql = "SELECT * FROM cfg_city";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		System.out.print(rs.getString("CITY_NAME"));
	    	}
	    	
		}catch (Exception e) {
				
				log.error("网格初始化类异常:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
	}

	public static boolean tableExists(Connection con, String tableName, int taskId) throws SQLException
	{
		if (Util.isNull(tableName)) return false;

		String prefix = taskId + " - ";

		Statement st = con.createStatement();
		st.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
		ResultSet rs = null;
		String sql = "select * from " + tableName + " where 1=2";
		try
		{
			rs = st.executeQuery(sql);
		}
		catch (SQLException e)
		{
			int code = e.getErrorCode();
			
			if ((code == 942) || (code == 208))
			{
				log.debug(prefix + "表或视图不存在,测试语句:" + sql + ",出现的异常信息:" + 
						e.getMessage().trim());
				return false;
			}
			log.debug(prefix + "测试表或视图是否存在时,发生异常,测试语句:" + sql + ",出现的异常信息:" + 
					e.getMessage().trim());
			return true;
		}
		catch (Exception e)
		{
			log.debug(prefix + "测试表或视图是否存在时,发生异常,测试语句:" + sql + ",出现的异常信息:" + 
					e.getMessage().trim());
			return true;
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
			}
			catch (Exception localException4)
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
		}
		catch (Exception localException5)
		{
		}
		return true;
	}

	public static boolean tableExists(Connection con, String tableName)
		throws SQLException
	{
		return tableExists(con, tableName, -1);
	}

	public static String getTableName(String sql)
	{
		String s = "";
		String str = sql.toLowerCase();
		s = str.substring(str.indexOf(" from ") + 5, str.length()).trim();
		int i = s.indexOf(" ");
		if (i > -1)
		{
			s = s.substring(0, i);
		}
		return s;
	}

	public static int getRowCount(Connection con, String selectStatement)
    	throws Exception
    {
		String sql = selectStatement.toLowerCase();
		int selectIndex = sql.indexOf("select ") + 7;
		int fromIndex = sql.indexOf(" from ");

		StringBuilder buffer = new StringBuilder();
		char[] chars = selectStatement.toCharArray();
		boolean flag = false;
		for (int i = 0; i < chars.length; i++)
		{
			if ((i >= selectIndex) && (i <= fromIndex) && (!flag))
			{
				buffer.append(" count(*) ");
				flag = true;
			} else {
				if ((i >= selectIndex) && (i <= fromIndex))
					continue;
				buffer.append(chars[i]);
			}
		}

		Statement st = con.createStatement();
		st.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
		ResultSet rs = st.executeQuery(buffer.toString());
		rs.next();
		int c = rs.getInt(1);
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
		}
		catch (Exception localException)
		{
		}
		return c;
    }

	public static void closeDBConnection(Connection con, Statement st, ResultSet rs)
	{
		if (rs != null)
		{
			try
			{
				rs.close();
			}
			catch (Exception localException)
			{
			}
		}
		if (st != null)
		{
			try
			{
				st.close();
			}
			catch (Exception localException1)
			{
			}
		}
		if (con != null)
		{
			try
			{
				con.close();
			}
			catch (Exception localException2)
			{
      		}
		}
	}
}
