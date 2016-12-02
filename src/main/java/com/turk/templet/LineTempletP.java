package com.turk.templet;

import com.turk.config.SystemConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 行解析模版
 * @author Administrator
 *
 */
public class LineTempletP extends AbstractTempletBase
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5790005519547062545L;

	/**
	 * 
	 */
	public Vector<SubTemplet> m_nTemplet = new Vector<SubTemplet>();
	
	/**
	 * 扫描类型,用于已何种方式查找对应模版中的某个表，或者文件。
	 * 0、通过FileName指定名称（char）匹配
	 * 1、
	 * 2、
	 * 3、自定义类型
	 */
	public int nScanType = 0;
	
	/**
	 * 扫描开始符号
	 */
	public String BeginSign = "";
	
	/**
	 * 扫描结束符号
	 */
	public String EndSign = "";
	
	/**
	 * 解析原始文件的编码格式，默认空 为当前服务器编码格式
	 */
	public String m_strEncode = "";

	
	
	public Vector<String> unReserved = new Vector<String>();

	public Map<Integer, String> columnMapping = new HashMap<Integer, String>();

	public void parseTemp(String tmpName)
    	throws Exception
    {
		if ((tmpName == null) || (tmpName.trim().equals(""))) {
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		String templetFilePath = null;
		templetFilePath = SystemConfig.getInstance().getTempletPath() + 
			File.separatorChar + tmpName;
		File file = new File(templetFilePath);
		doc = builder.parse(file);

		NodeList pn = doc.getElementsByTagName("PUBLIC");
		if (pn.getLength() >= 1)
		{
			this.nScanType = Integer.parseInt(doc.getElementsByTagName("SCANTYPE").item(0).getFirstChild().getNodeValue());
			if ((doc.getElementsByTagName("ENCODE") != null) && 
					(doc.getElementsByTagName("ENCODE").item(0) != null) && 
					(doc.getElementsByTagName("ENCODE").item(0).getFirstChild() != null))
			{
				this.m_strEncode = doc.getElementsByTagName("ENCODE").item(0).getFirstChild().getNodeValue();
			}
		}

		NodeList nl = doc.getElementsByTagName("UNSTR");
		for (int i = 0; i < nl.getLength(); i++)
		{
			String m_NodeValue = doc.getElementsByTagName("UNSTR").item(i).getFirstChild().getNodeValue();
			this.unReserved.add(m_NodeValue);
		}
		
		
		

		NodeList nl2 = doc.getElementsByTagName("RITEM");
		for (int i = 0; i < nl2.getLength(); i++)
		{
			SubTemplet SubTemp = new SubTemplet();
			
			String strFileName = "";
			if ((doc.getElementsByTagName("FILENAME").item(i) != null) && 
					(doc.getElementsByTagName("FILENAME").item(i).getFirstChild() != null)) {
				strFileName = doc.getElementsByTagName("FILENAME").item(i).getFirstChild().getNodeValue();
			}
			
			String strTag = "";
			if ((doc.getElementsByTagName("TAG").item(i) != null) && 
					(doc.getElementsByTagName("TAG").item(i).getFirstChild() != null)) {
				strTag = doc.getElementsByTagName("TAG").item(i).getFirstChild().getNodeValue();
			}
			
			int nFileNameCompare = 0;
			if ((doc.getElementsByTagName("FILENAMECOMPARE").item(i) != null) && 
					(doc.getElementsByTagName("FILENAMECOMPARE").item(i).getFirstChild() != null)) {
				nFileNameCompare = Integer.parseInt(doc.getElementsByTagName("FILENAMECOMPARE").item(i).getFirstChild().getNodeValue());
			}
			
			String strRawColumnList = "";
			if ((doc.getElementsByTagName("COLUMNLISTSIGN").item(i) != null) && 
					(doc.getElementsByTagName("COLUMNLISTSIGN").item(i).getFirstChild() != null))
				strRawColumnList = doc.getElementsByTagName("COLUMNLISTSIGN").item(i).getFirstChild().getNodeValue();
     
			String strColumnsAppend = "";
			if ((doc.getElementsByTagName("APPENDCOLUMNLIST").item(i) != null) && 
					(doc.getElementsByTagName("APPENDCOLUMNLIST").item(i).getFirstChild() != null))
				strColumnsAppend = doc.getElementsByTagName("APPENDCOLUMNLIST").item(i).getFirstChild().getNodeValue();
      
			String LineHeadSign = "";
			if ((doc.getElementsByTagName("LINEHEADSIGN").item(i) != null) && 
					(doc.getElementsByTagName("LINEHEADSIGN").item(i).getFirstChild() != null))
				LineHeadSign = doc.getElementsByTagName("LINEHEADSIGN").item(i).getFirstChild().getNodeValue();
      
			int LineHeadType = Integer.parseInt(doc.getElementsByTagName("LINEHEADTYPE").item(i).getFirstChild().getNodeValue());
			int nParseType = Integer.parseInt(doc.getElementsByTagName("PARSETYPE").item(i).getFirstChild().getNodeValue());
			int m_nColumnCount = Integer.parseInt(doc.getElementsByTagName("COLUMNCOUNT").item(i).getFirstChild().getNodeValue());
			
			int nDefaultColumnType = 0;
			if ((doc.getElementsByTagName("DEFAULTCOLUMNTYPE") != null) && 
					(doc.getElementsByTagName("DEFAULTCOLUMNTYPE").item(i) != null) && 
					(doc.getElementsByTagName("DEFAULTCOLUMNTYPE").item(i).getFirstChild() != null))
			{
				 nDefaultColumnType = Integer.parseInt(doc.getElementsByTagName("DEFAULTCOLUMNTYPE").item(i).getFirstChild().getNodeValue());
			}
			
			
			//默认配置为分号;
			String m_FieldSplitSign =";";
			if ((doc.getElementsByTagName("FIELDSPLITSIGN") != null) && 
					(doc.getElementsByTagName("FIELDSPLITSIGN").item(i) != null) && 
					(doc.getElementsByTagName("FIELDSPLITSIGN").item(i).getFirstChild() != null))
			{
				 m_FieldSplitSign = doc.getElementsByTagName("FIELDSPLITSIGN").item(i).getFirstChild().getNodeValue();
			}
			
			String m_NewFieldSplitSign = doc.getElementsByTagName("NEWFIELDSPLITSIGN").item(i).getFirstChild().getNodeValue();
			String m_FieldUpSplitSign = "";

			if ((doc.getElementsByTagName("FIELDUPSPLITSIGN") != null) && 
					(doc.getElementsByTagName("FIELDUPSPLITSIGN").item(i) != null) && 
					(doc.getElementsByTagName("FIELDUPSPLITSIGN").item(i).getFirstChild() != null))
			{
				m_FieldUpSplitSign = doc.getElementsByTagName("FIELDUPSPLITSIGN").item(i).getFirstChild().getNodeValue();
			}

			if ((doc.getElementsByTagName("ESCAPCHAR") != null) && 
					(doc.getElementsByTagName("ESCAPCHAR").item(i) != null) && 
					(doc.getElementsByTagName("ESCAPCHAR").item(i).getFirstChild() != null))
			{
				String strEscape = doc.getElementsByTagName("ESCAPCHAR").item(i).getFirstChild().getNodeValue();
				if ((strEscape != null) && (strEscape.equals("0"))) {
					SubTemp.m_bEscape = false;
				}
			}
			
			

			String nvl = "0";
			if ((doc.getElementsByTagName("nvl") != null) && 
					(doc.getElementsByTagName("nvl").item(i) != null))
			{
				if (doc.getElementsByTagName("nvl").item(i).getFirstChild() == null)
					nvl = "";
				else
					nvl = doc.getElementsByTagName("nvl").item(i).getFirstChild().getNodeValue();
			}
      
			SubTemp.nvl = nvl;

			SubTemp.m_strFileName = strFileName;
			SubTemp.m_tag = strTag;
			SubTemp.m_nFileNameCompare = nFileNameCompare;

			SubTemp.m_RawColumnList = strRawColumnList;
			SubTemp.m_ColumnListAppend = strColumnsAppend;
			if (LineHeadSign.equals("NULL"))
				SubTemp.m_strLineHeadSign = "";
			else {
				SubTemp.m_strLineHeadSign = LineHeadSign;
			}
			if ((doc.getElementsByTagName("HASROWKEY") != null) && 
					(doc.getElementsByTagName("HASROWKEY").item(i) != null) && 
					(doc.getElementsByTagName("HASROWKEY").item(i).getFirstChild() != null))
			{
				String strEscape = doc.getElementsByTagName("HASROWKEY").item(i).getFirstChild().getNodeValue();
				if ((strEscape != null) && (strEscape.equals("1"))) {
					SubTemp.m_hasRowkey = true;
				}
			}
			
			SubTemp.m_nLineHeadType = LineHeadType;
			SubTemp.m_nColumnCount = m_nColumnCount;
			SubTemp.m_strFieldSplitSign = m_FieldSplitSign;
			SubTemp.m_strFieldUpSplitSign = m_FieldUpSplitSign;
			SubTemp.m_strNewFieldSplitSign = m_NewFieldSplitSign;
			SubTemp.m_nParseType = nParseType;
			SubTemp.m_nDefaultColumnType = nDefaultColumnType;
			

			if ((doc.getElementsByTagName("COLUMNS").item(i) != null) && 
					(doc.getElementsByTagName("COLUMNS").item(i).getFirstChild() != null))
			{
				Node fieldnode = doc.getElementsByTagName("COLUMNS").item(i);
				parseFieldInfo(SubTemp.m_Filed, fieldnode);
			}

			this.m_nTemplet.add(SubTemp);
		}
    }

  	private void parseFieldInfo(Map<Integer, FieldTemplet> tableInfo, Node currentNode)
  	{
  		NodeList Ssn = currentNode.getChildNodes();

  		for (int nIndex = 0; nIndex < Ssn.getLength(); nIndex++)
  		{
  			Node tempnode = Ssn.item(nIndex);

  			if ((tempnode.getNodeType() != 1) || 
  					(!tempnode.getNodeName().toUpperCase().equals("FIELDITEM")))
  				continue;
  			NodeList childnodeList = tempnode.getChildNodes();
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
  					else {
  						field.m_nFieldIndex = Integer.parseInt(strValue);
  					}
  				}
  				else if (NodeName.equals("FIELDNAME"))
  				{
  					field.m_strFieldName = strValue;
  				}
  				else if (NodeName.equals("STARTPOS"))
  				{
  					if(strValue == null || strValue.equals(""))
  						field.m_nStartPos = 0;
  					else
  						field.m_nStartPos = Integer.parseInt(strValue);
  				}
  				else if (NodeName.equals("DATALENGTH"))
  				{
  					if(strValue == null || strValue.equals(""))
  						field.m_nDataLength = 0;
  					else
  						field.m_nDataLength = Integer.parseInt(strValue);
  				}
  				else if (NodeName.equals("FIELDTYPE"))
  				{
  					field.m_type = strValue;
  				} else {
  					if (!NodeName.equals("DATEFORMAT"))
  						continue;
  					field.m_dateFormat = strValue;
  				}
  			}

  			tableInfo.put(Integer.valueOf(field.m_nFieldIndex), field);
  			this.columnMapping.put(Integer.valueOf(field.m_nFieldIndex), field.m_strFieldName);
  		}
  	}

  	public static void main(String[] args)
  	{
  		new LineTempletP().buildTmp(9);
  	}

  	/**
  	 * 解析字段模版
  	 * @author Administrator
  	 *
  	 */
  	public class FieldTemplet
  	{
  		/**
  		 * 字段索引
  		 */
  		public int m_nFieldIndex;
  		
  		/**
  		 * 字段名称
  		 */
  		public String m_strFieldName = "";
  		
  		/**
  		 * 解析起始位置
  		 */
  		public int m_nStartPos = 0;
  		
  		/**
  		 * 字段长度
  		 */
  		public int m_nDataLength = 0;
  		
  		/**
  		 * 字段类型 DATE,INT,CHAR
  		 */
  		public String m_type;
  		
  		/**
  		 * 字段格式
  		 */
  		public String m_dateFormat;

  		public FieldTemplet()
  		{
  		}
  	}

  	/**
  	 * 解析子模版
  	 * @author Administrator
  	 *
  	 */
  	public class SubTemplet
  	{
  		/**
  		 * 文件名
  		 */
  		public String m_strFileName = "";
  		
  		/**
  		 * 标识，特殊需要使用
  		 */
  		public String m_tag = "";
  		
  		/**
  		 * 是否需要文件名比较
  		 */
  		public int m_nFileNameCompare = 0;
  		
  		/**
  		 * 
  		 */
  		public String m_RawColumnList = "";
  		
  		/**
  		 * 
  		 */
  		public String m_ColumnListAppend = "";
  		
  		/**
  		 * 每一行头位置标识符
  		 */
  		public String m_strLineHeadSign = "";
  		
  		/**
  		 * 此处表示为每个子模版的索引编号
  		 */
  		public int m_nLineHeadType = 0;
  		
  		/**
  		 * 解析字段列数
  		 */
  		public int m_nColumnCount = 0;
  		
  		/**
  		 * 解析原始文件字段分隔符
  		 */
  		public String m_strFieldSplitSign = "";
  		
  		/**
  		 * 解析原始文件字段分隔符，特殊字符例如引号中间的逗号
  		 */
  		public String m_strFieldUpSplitSign = "";
  		
  		/**
  		 * 
  		 */
  		public boolean m_bEscape = true;
  		
  		/**
  		 * 解析后生成的字段分隔符
  		 */
  		public String m_strNewFieldSplitSign = "";
  		
  		/**
  		 * 解析类型  对应解析配置文件中  RESERVED.RITEM.PARSETYPE 节
  		 * 1：按行间隔符解析
  		 * 2: 按位解析，字符串起始位置+字段长度解析
  		 * 3：整行解析，只做间隔符替换         
  		 */
  		public int m_nParseType;
  		
  		/**
  		 * 默认解析行自带默认字段类型
  		 * 0：不带默认字段(默认)
  		 * 1：带默认字段 DEVICEID,COLLECTTIME,STAMPTIME,
  		 * 2：带默认字段 START_TIME 当前采集任务的时间
  		 */
  		public int m_nDefaultColumnType;
  		
  		/**
  		 * 用于hbase的 rowkey
  		 */
  		public boolean m_hasRowkey = false;
  		
  		
  		
  		/**
  		 * 解析字段
  		 */
  		public Map<Integer, LineTempletP.FieldTemplet> m_Filed = new HashMap<Integer, FieldTemplet>();

  		/**
  		 * 当字段为空时，替代字段的符号
  		 */
  		public String nvl = "0";

  		public SubTemplet()
  		{
  		}
  	}
}