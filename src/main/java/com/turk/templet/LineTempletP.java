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
 * �н���ģ��
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
	 * ɨ������,�����Ѻ��ַ�ʽ���Ҷ�Ӧģ���е�ĳ���������ļ���
	 * 0��ͨ��FileNameָ�����ƣ�char��ƥ��
	 * 1��
	 * 2��
	 * 3���Զ�������
	 */
	public int nScanType = 0;
	
	/**
	 * ɨ�迪ʼ����
	 */
	public String BeginSign = "";
	
	/**
	 * ɨ���������
	 */
	public String EndSign = "";
	
	/**
	 * ����ԭʼ�ļ��ı����ʽ��Ĭ�Ͽ� Ϊ��ǰ�����������ʽ
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
			
			
			//Ĭ������Ϊ�ֺ�;
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
  	 * �����ֶ�ģ��
  	 * @author Administrator
  	 *
  	 */
  	public class FieldTemplet
  	{
  		/**
  		 * �ֶ�����
  		 */
  		public int m_nFieldIndex;
  		
  		/**
  		 * �ֶ�����
  		 */
  		public String m_strFieldName = "";
  		
  		/**
  		 * ������ʼλ��
  		 */
  		public int m_nStartPos = 0;
  		
  		/**
  		 * �ֶγ���
  		 */
  		public int m_nDataLength = 0;
  		
  		/**
  		 * �ֶ����� DATE,INT,CHAR
  		 */
  		public String m_type;
  		
  		/**
  		 * �ֶθ�ʽ
  		 */
  		public String m_dateFormat;

  		public FieldTemplet()
  		{
  		}
  	}

  	/**
  	 * ������ģ��
  	 * @author Administrator
  	 *
  	 */
  	public class SubTemplet
  	{
  		/**
  		 * �ļ���
  		 */
  		public String m_strFileName = "";
  		
  		/**
  		 * ��ʶ��������Ҫʹ��
  		 */
  		public String m_tag = "";
  		
  		/**
  		 * �Ƿ���Ҫ�ļ����Ƚ�
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
  		 * ÿһ��ͷλ�ñ�ʶ��
  		 */
  		public String m_strLineHeadSign = "";
  		
  		/**
  		 * �˴���ʾΪÿ����ģ����������
  		 */
  		public int m_nLineHeadType = 0;
  		
  		/**
  		 * �����ֶ�����
  		 */
  		public int m_nColumnCount = 0;
  		
  		/**
  		 * ����ԭʼ�ļ��ֶηָ���
  		 */
  		public String m_strFieldSplitSign = "";
  		
  		/**
  		 * ����ԭʼ�ļ��ֶηָ����������ַ����������м�Ķ���
  		 */
  		public String m_strFieldUpSplitSign = "";
  		
  		/**
  		 * 
  		 */
  		public boolean m_bEscape = true;
  		
  		/**
  		 * ���������ɵ��ֶηָ���
  		 */
  		public String m_strNewFieldSplitSign = "";
  		
  		/**
  		 * ��������  ��Ӧ���������ļ���  RESERVED.RITEM.PARSETYPE ��
  		 * 1�����м��������
  		 * 2: ��λ�������ַ�����ʼλ��+�ֶγ��Ƚ���
  		 * 3�����н�����ֻ��������滻         
  		 */
  		public int m_nParseType;
  		
  		/**
  		 * Ĭ�Ͻ������Դ�Ĭ���ֶ�����
  		 * 0������Ĭ���ֶ�(Ĭ��)
  		 * 1����Ĭ���ֶ� DEVICEID,COLLECTTIME,STAMPTIME,
  		 * 2����Ĭ���ֶ� START_TIME ��ǰ�ɼ������ʱ��
  		 */
  		public int m_nDefaultColumnType;
  		
  		/**
  		 * ����hbase�� rowkey
  		 */
  		public boolean m_hasRowkey = false;
  		
  		
  		
  		/**
  		 * �����ֶ�
  		 */
  		public Map<Integer, LineTempletP.FieldTemplet> m_Filed = new HashMap<Integer, FieldTemplet>();

  		/**
  		 * ���ֶ�Ϊ��ʱ������ֶεķ���
  		 */
  		public String nvl = "0";

  		public SubTemplet()
  		{
  		}
  	}
}