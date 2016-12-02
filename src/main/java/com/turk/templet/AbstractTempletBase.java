package com.turk.templet;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.templet.LineTempletP.SubTemplet;
import com.turk.util.CommonDB;
import com.turk.util.LogMgr;

/**
 * 模版抽象类，所有模版继承此类
 * @author Administrator
 *
 */
public abstract class AbstractTempletBase
  implements TempletBase,Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5592326653795904033L;
	public int tmpID = 0;
	public String tmpName = null;
	public String edition = null;
	public String tmpFileName = null;
	public int tmpType;
	protected static Logger log = LogMgr.getInstance().getSystemLogger();

	
	
	public void buildTmp(int tmpID)
	{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try
		{
			conn = CommonDB.getConnection();
			if (conn == null)
			{
				log.error("模板解析失败(模板编号=" + tmpID + "),原因:无法获取数据库连接.");
				return;
			}
			
			String sql = "select * from UTL_CONF_TEMPLET t where TMPID =" + tmpID;
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			if (rs.next())
			{
				this.tmpID = tmpID;
				this.tmpName = rs.getString("TMPNAME");
				this.edition = rs.getString("EDITION");
				this.tmpFileName = rs.getString("TEMPFILENAME");
				this.tmpType = rs.getInt("TMPTYPE");
				parseTemp(this.tmpFileName);
			}
		}
		catch (Exception e)
		{
			log.error("模板解析失败(模板编号=" + tmpID + "),原因:", e);
			try
			{
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			}
			catch (SQLException localSQLException1)
			{
			}
		}
		finally
		{
			try
			{
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			}
			catch (SQLException localSQLException2)
			{
			}
		}
 	}

	public void buildTmp(TempletRecord record)
	{
		if (record != null)
		{
			this.tmpID = record.getId();
			this.edition = record.getEdition();
			this.tmpName = record.getName();
			this.tmpFileName = record.getFileName();
			try
			{
				parseTemp(this.tmpFileName);
			}
			catch (Exception e)
			{
				log.error("模板解析失败(模板编号=" + this.tmpID + "),原因:", e);
			}
		}
	}

	public abstract void parseTemp(String paramString)
		throws Exception;

	protected String getNodeValue(Node CurrentNode)
	{
		String strValue = "";
		NodeList nodelist = CurrentNode.getChildNodes();
		if (nodelist != null)
		{
			for (int i = 0; i < nodelist.getLength(); i++)
			{
				Node tempnode = nodelist.item(i);
				if (tempnode.getNodeType() != 3)
					continue;
				strValue = tempnode.getNodeValue();
			}
		}
		return strValue;
	}

	protected boolean existSubField(Node CurrentNode)
	{
		boolean IsExist = false;
		NodeList nodelist = CurrentNode.getChildNodes();
		if (nodelist != null)
		{
			for (int i = 0; i < nodelist.getLength(); i++)
			{
				Node tempnode = nodelist.item(i);
				if (tempnode.getNodeType() != 1)
					continue;
				NodeList sublist = tempnode.getChildNodes();
				if (sublist == null)
					continue;
				IsExist = true;
			}
		}
		return IsExist;
	}

	protected String WrapPromptChange(String Keyword)
	{
		char ch = '\n';
		int nIndex = Keyword.indexOf("\\n");
		while (nIndex >= 0)
		{
			String Head = Keyword.substring(0, nIndex);

			String Tail = "";
			if (nIndex + 2 < Keyword.length()) {
				Tail = Keyword.substring(nIndex + 2, Keyword.length());
			}
			Keyword = Head + String.valueOf(ch) + Tail;
			nIndex = Keyword.indexOf("\\n");
		}

		Keyword = Keyword.replaceAll("\\|", "\\\\|");
		return Keyword;
	}
	
	/**
	 * 
	 */
	protected Vector<SubTemplet> m_nTemplet = new Vector<SubTemplet>();
}