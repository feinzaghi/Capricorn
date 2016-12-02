package com.turk.distributor;

import com.turk.config.SystemConfig;
import com.turk.templet.AbstractTempletBase;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class DistributeTemplet extends AbstractTempletBase
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2242060525465756137L;
	public String dbDriver = "";
	public String dbUrl = "";
	public String dbDataBase = "";
	public String dbUserName = "";
	public String dbPwd = "";
	
	public String encode = "";

	/**
	 * 分发类型，1,2,3:SQL,4:File
	 */
	public int stockStyle = 0;
	
	/**
	 * 一次提交的行数
	 */
	public int onceStockCount = 0;

	public Map<Integer, TableTemplet> tableTemplets = new HashMap<Integer, TableTemplet>();

	public Map<Integer, TableItem> tableItems = new HashMap<Integer, TableItem>();

	/**
	 * 加载模版
	 */
	public void parseTemp(String distFileName)
		throws Exception
    {
		if ((distFileName == null) || (distFileName.trim().equals(""))) {
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		String templetFilePath = SystemConfig.getInstance().getTempletPath() + 
			File.separatorChar + distFileName;
		doc = builder.parse(new File(templetFilePath));

		NodeList publicNodeList = doc.getElementsByTagName("PUBLIC");
		if (publicNodeList.getLength() >= 1)
		{
			if (doc.getElementsByTagName("DRIVER").item(0).getFirstChild() == null)
				this.dbDriver = "";
			else {
				this.dbDriver = doc.getElementsByTagName("DRIVER").item(0).getFirstChild().getNodeValue();
			}
			if (doc.getElementsByTagName("DRIVERURL").item(0).getFirstChild() == null)
				this.dbUrl = "";
			else {
				this.dbUrl = doc.getElementsByTagName("DRIVERURL").item(0).getFirstChild().getNodeValue();
			}
			if (doc.getElementsByTagName("DATABASE").item(0).getFirstChild() == null)
				this.dbDataBase = "";
			else {
				this.dbDataBase = doc.getElementsByTagName("DATABASE").item(0).getFirstChild().getNodeValue();
			}
			if (doc.getElementsByTagName("USERNAME").item(0).getFirstChild() == null)
				this.dbUserName = "";
			else {
				this.dbUserName = doc.getElementsByTagName("USERNAME").item(0).getFirstChild().getNodeValue();
			}
			if (doc.getElementsByTagName("PASSWORD").item(0).getFirstChild() == null)
				this.dbPwd = "";
			else {
				this.dbPwd = doc.getElementsByTagName("PASSWORD").item(0).getFirstChild().getNodeValue();
			}
			if (doc.getElementsByTagName("STOCKSTYLE").item(0).getFirstChild() == null)
				this.stockStyle = 0;
			else {
				this.stockStyle = Integer.parseInt(doc.getElementsByTagName("STOCKSTYLE").item(0).getFirstChild().getNodeValue());
			}
			if (doc.getElementsByTagName("ONCESTOCKCOUNT").item(0).getFirstChild() == null)
				this.onceStockCount = 0;
			else {
				this.onceStockCount = Integer.parseInt(doc.getElementsByTagName("ONCESTOCKCOUNT").item(0).getFirstChild().getNodeValue());
			}
			
			if ((doc.getElementsByTagName("ENCODE") != null) && 
					(doc.getElementsByTagName("ENCODE").item(0) != null) && 
					(doc.getElementsByTagName("ENCODE").item(0).getFirstChild() != null))
			{
				this.encode = doc.getElementsByTagName("ENCODE").item(0).getFirstChild().getNodeValue();
			}
		}

		NodeList tableNodeInfo = doc.getElementsByTagName("DATATABLE");
		for (int i = 0; i < tableNodeInfo.getLength(); i++)
		{
			TableTemplet table = new TableTemplet();
			
			if (doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild() == null)
				table.tableIndex = 0;
			else
			{
				try
				{
					table.tableIndex = Integer.parseInt(doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild().getNodeValue());
				}
				catch (Exception localException) {
				}
			}
			
			if (doc.getElementsByTagName("TABLENAME").item(i).getFirstChild() == null)
				table.tableName = "";
			else {
				table.tableName = doc.getElementsByTagName("TABLENAME").item(i).getFirstChild().getNodeValue();
			}
			if ((doc.getElementsByTagName("FILLTITLE") != null) && 
					(doc.getElementsByTagName("FILLTITLE").item(i) != null) && 
					(doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild() != null))
			{
				String strValue = doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild().getNodeValue();
				if (Integer.parseInt(strValue) == 1) {
					table.isFillTitle = true;
				}
			}
			
			if((doc.getElementsByTagName("TABLETAG") != null) && 
					(doc.getElementsByTagName("TABLETAG").item(i) != null) && 
					(doc.getElementsByTagName("TABLETAG").item(i).getFirstChild() != null))
			{
				table.m_TableTag = doc.getElementsByTagName("TABLETAG").item(i).getFirstChild().getNodeValue();
			}
			
			if((doc.getElementsByTagName("ISCALU") != null) && 
    	        (doc.getElementsByTagName("ISCALU").item(i) != null) && 
    	        (doc.getElementsByTagName("ISCALU").item(i).getFirstChild() != null))
			{
				String strValue1 = doc.getElementsByTagName("ISCALU").item(i).getFirstChild().getNodeValue();
				if (Integer.parseInt(strValue1) == 1) 
					table.isCalu = true;
			}
			
			if((doc.getElementsByTagName("ISROWKEY") != null) && 
	    	        (doc.getElementsByTagName("ISROWKEY").item(i) != null) && 
	    	        (doc.getElementsByTagName("ISROWKEY").item(i).getFirstChild() != null))
				{
					String strValue1 = doc.getElementsByTagName("ISROWKEY").item(i).getFirstChild().getNodeValue();
					if (Integer.parseInt(strValue1) == 1) 
						table.isRowkey = true;
				}
	  
		  
			if((doc.getElementsByTagName("BAKDIRECTORY") != null) && 
					(doc.getElementsByTagName("BAKDIRECTORY").item(i) != null) && 
					(doc.getElementsByTagName("BAKDIRECTORY").item(i).getFirstChild() != null))
			{
				table.bakDirectory = doc.getElementsByTagName("BAKDIRECTORY").item(i).getFirstChild().getNodeValue();
			}
			
			if((doc.getElementsByTagName("UPLOADPATH") != null) && 
					(doc.getElementsByTagName("UPLOADPATH").item(i) != null) && 
					(doc.getElementsByTagName("UPLOADPATH").item(i).getFirstChild() != null))
			{
				table.UploadPath = doc.getElementsByTagName("UPLOADPATH").item(i).getFirstChild().getNodeValue();
			}
			
			if((doc.getElementsByTagName("NEWFIELDSPLITSIGN") != null) && 
					(doc.getElementsByTagName("NEWFIELDSPLITSIGN").item(i) != null) && 
					(doc.getElementsByTagName("NEWFIELDSPLITSIGN").item(i).getFirstChild() != null))
			{
				table.m_strNewFieldSplitSign = doc.getElementsByTagName("NEWFIELDSPLITSIGN").item(i).getFirstChild().getNodeValue();
			}
      
			if ((doc.getElementsByTagName("FIELDS").item(i) != null) && 
					(doc.getElementsByTagName("FIELDS").item(i).getFirstChild() != null))
			{
				Node fieldsNode = doc.getElementsByTagName("FIELDS").item(i);
				parseFieldInfo(table.fields, fieldsNode);
			}
			
			if((doc.getElementsByTagName("PRESQL") != null) && 
					(doc.getElementsByTagName("PRESQL").item(i) != null) && 
					(doc.getElementsByTagName("PRESQL").item(i).getFirstChild() != null))
			{
				table.m_presql = doc.getElementsByTagName("PRESQL").item(i).getFirstChild().getNodeValue();
			}
			
			if((doc.getElementsByTagName("POSTSQL") != null) && 
					(doc.getElementsByTagName("POSTSQL").item(i) != null) && 
					(doc.getElementsByTagName("POSTSQL").item(i).getFirstChild() != null))
			{
				table.m_postsql = doc.getElementsByTagName("POSTSQL").item(i).getFirstChild().getNodeValue();
			}
			
			this.tableTemplets.put(Integer.valueOf(table.tableIndex), table);
		}
    }

	private void parseFieldInfo(Map<Integer, FieldTemplet> fields, Node fieldsNode)
	{
		NodeList allFileds = fieldsNode.getChildNodes();

		for (int nIndex = 0; nIndex < allFileds.getLength(); nIndex++)
		{
			Node fieldNode = allFileds.item(nIndex);

			if ((fieldNode.getNodeType() != 1) || 
					(!fieldNode.getNodeName().toUpperCase().equals("FIELDITEM")))
				continue;
			NodeList childnodeList = fieldNode.getChildNodes();
			if (childnodeList == null)
				continue;
			FieldTemplet field = new FieldTemplet();
			for (int i = 0; i < childnodeList.getLength(); i++)
			{
				Node childnode = childnodeList.item(i);
				if (childnode.getNodeType() != 1)
					continue;
				String NodeName = childnode.getNodeName().toUpperCase();
				String strValue = getNodeValue(childnode);
				if (NodeName.equals("FIELDINDEX"))
				{
					if ((strValue == null) || (strValue.equals("")))
						field.m_nFieldIndex = 0;
					else
						field.m_nFieldIndex = Integer.parseInt(strValue);
				}
				else if (NodeName.equals("FIELDMAPPING"))
				{
					field.m_strFieldMapping = strValue;
				}
				else if (NodeName.equals("FIELDNAME"))
				{
					field.m_strFieldName = strValue;
				}
				else if (NodeName.equals("RAWNAME"))
				{
					field.rawName = strValue;
				}
				else if (NodeName.equals("ISKEY"))
				{
					if (strValue.trim().equals("1"))
						field.m_bIsKey = true;
				}
				else if (NodeName.equals("KEYVALUE"))
				{
					field.m_strKeyValue = strValue;
				}
				else if (NodeName.equals("ISDEFAULT"))
				{
					if (strValue.trim().equals("1"))
						field.m_bIsDefault = true;
				}
				else if (NodeName.equals("DEFAULTVALUE"))
				{
					field.m_strDefaultValue = strValue;
				}
				else if (NodeName.equals("DATATYPE"))
				{
					if ((strValue == null) || (strValue.equals("")))
						field.m_nDataType = 0;
					else
						field.m_nDataType = Integer.parseInt(strValue);
				}
				else if(NodeName.equals("DATALENGTH"))
				{	//Turk Add 2011.4.3 分发字段长度
					if((strValue !=null)&&(!strValue.equals("")))
						field.m_DataLength = Integer.parseInt(strValue);
				} 
				else {
					if (!NodeName.equals("DATATIMEFORMAT"))
						continue;
					field.m_strDataTimeFormat = strValue;
				}
			}

			fields.put(Integer.valueOf(field.m_nFieldIndex), field);
		}
	}

	public class FieldTemplet
	{
	    public int m_nFieldIndex = 0;
	    public String m_strFieldMapping = "";
	    public String m_strFieldName = "";
	    public boolean m_bIsKey = false;
	    public String m_strKeyValue = "";
	    public boolean m_bIsDefault = false;
	    public String m_strDefaultValue = "";
	    public int m_nDataType = 0;
	    public String m_strDataTimeFormat = "";
	    public String rawName = "";
	    public int m_DataLength = 0;

	    public FieldTemplet()
	    {
	    }
	}

	public class TableTemplet
	{
	    public int tableIndex;
	    public String tableName;
	    public boolean isFillTitle = false;
	    /**
	     * 是否用于统计计算
	     */
	    public boolean isCalu = false;
	    /**
	     * 用于hbase生成rowkey
	     */
	    public boolean isRowkey = false;
	    /**
	     * 原始文件备份到本地程序下的目录
	     */
	    public String bakDirectory = "";
	    
	    /**
	     * 分发的数据间隔符
	     */
	    public String m_strNewFieldSplitSign = "";
	    
	    /**
	     * 数据表的标识字段，以备特殊判断使用
	     */
	    public String m_TableTag = "";
	    
	    /**
	     * 入库前需要执行的SQL
	     */
	    public String m_presql = "";
	    
	    /**
	     * 入库后需要执行的SQL
	     */
	    public String m_postsql = "";
	    
	    /**
	     * 解析文件上传目录
	     */
	    public String UploadPath = "";
	    
	    public Map<Integer, DistributeTemplet.FieldTemplet> fields = new HashMap<Integer, FieldTemplet>();

	    public TableTemplet()
	    {
	    }
	}
}