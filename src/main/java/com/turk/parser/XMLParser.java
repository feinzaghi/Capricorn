package com.turk.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import javax.xml.parsers.ParserConfigurationException;   
import javax.xml.parsers.SAXParser;   

import org.xml.sax.Attributes;   
import org.xml.sax.SAXException;   
import org.xml.sax.helpers.DefaultHandler;   

import com.turk.distributor.Distribute;
import com.turk.task.CollectObjInfo;
import com.turk.templet.XMLTempletP;
import com.turk.util.LogMgr;

public class XMLParser extends Parser
{
	public XMLParser()
	{
	}

	public XMLParser(CollectObjInfo collectInfo)
	{
		super(collectInfo);
	}

	protected String normalizeString(String s)
	{
		s = s.trim();
		s = s.replaceAll(";", " ");
		s = s.replaceAll("\n", " ");
		return s;
	}
	
	private final static String registrations[] = {
        "-//Sun Microsystems, Inc.//DTD Web Application 2.2//EN",
        "/web-app_2_2.dtd",
        "-//Sun Microsystems, Inc.//DTD Web Application 2.3//EN",
        "/web-app_2_3.dtd"
	};

	public boolean parseData()
    	throws Exception
    {
		
		SAXParserFactory saxfac = SAXParserFactory.newInstance();   
		
		try 
		{   

			SAXParser saxparser = saxfac.newSAXParser();   
			InputStream is = new FileInputStream(this.fileName);   
			ParserObj pObj = new ParserObj();
			
			saxparser.parse(is, new MySAXHandler(this.collectObjInfo,
					pObj,this.distribute));   

		} catch (ParserConfigurationException e) {   
			e.printStackTrace();   
		} catch (SAXException e) {   
			e.printStackTrace();   
		} catch (FileNotFoundException e) {  
			e.printStackTrace();   
		} catch (IOException e) {   
			e.printStackTrace();   
		}   
		
		
		/*
		Document doc = null; 
		File inputXml=new File(fileName);   
		SAXReader saxReader = new SAXReader();   
		try {   
			doc = saxReader.read(inputXml);  
		} catch (DocumentException e) {   
			System.out.println(e.getMessage());   
		}   
		System.out.println("dom4j parserXml");   
			
		*/

		
		/*
		XMLTempletP templet = (XMLTempletP)this.collectObjInfo.getParseTemplet();
		for (XMLTempletP.ParseTag parseTag : templet.m_listParseTag)
		{
			NodeList nl = doc.getElementsByTagName(parseTag.m_strTagName);
			for (int i = 0; i < nl.getLength(); i++)
			{
				Node node = nl.item(i);

				for (XMLTempletP.Table table : parseTag.m_listTable)
				{
					switch (table.m_nSrcIDType)
					{
						case 1:
							break;
						case 2:
							Node attrib = node.getAttributes().getNamedItem(table.m_strAttributeName);
							if (attrib == null)
								continue;
							String attribValue = attrib.getFirstChild().getNodeValue();
							if (attribValue.equals(table.m_strAttributeValue)) break;
							break;
						default:
							break;
					}

					StringBuffer strNewRow = new StringBuffer();

					strNewRow.append(this.collectObjInfo.getDevInfo().getDevID());
					strNewRow.append(";");

					Date now = new Date();
					SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String strTime = spformat.format(now);
					strNewRow.append(strTime + ";");

					strTime = spformat.format(this.collectObjInfo.getLastCollectTime());
					strNewRow.append(strTime + ";");
					try
					{
						switch (table.m_nGenColumnType)
						{
							case 1:
								strNewRow = parseTableLines1(table, node, strNewRow);
								break;
							case 2:
								strNewRow = parseTableLines2(table, node, strNewRow);
								break;
							case 3:
								strNewRow = parseTableLines3(table, node, strNewRow);
						}

					}
					catch (Exception e)
					{
						e.printStackTrace();
					}

					this.distribute.DistributeData(strNewRow.toString().getBytes(), table.m_nTableIndex);
				}
			}
		}
	*/
		return true;
    }

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}

	
	/**
	 * 只有键值关系
	 * @param table
	 * @param node
	 * @param strNewRow
	 * @return
	 */
	/*private StringBuffer parseTableLines1(XMLTempletP.Table table, Node node, StringBuffer strNewRow)
	{
		StringBuffer strLine = new StringBuffer();

		String strTemp = node.getFirstChild().getNodeValue();
		strLine.append(normalizeString(strTemp.trim()));

		if (table.m_listFields.size() != 0)
		{
			String[] fields = strLine.toString().split(";");

			for (XMLTempletP.FieldItem field : table.m_listFields)
			{
				if ((field.m_nFieldIndex > 0) && 
						(field.m_nFieldIndex < fields.length))
				{
					strNewRow.append(fields[field.m_nFieldIndex] + ";");
				}
				else {
					strNewRow.append(";");
				}
			}
		}
		strNewRow.append("\n");
		return strNewRow;
	}

	private StringBuffer parseTableLines2(XMLTempletP.Table table, Node node, StringBuffer strNewRow)
	{
		StringBuffer strLine = new StringBuffer();
		Node child2;
		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
		{
			if ((child.getNodeType() != 1) || 
					(!child.getNodeName().equals(table.m_strSubTagName)))
				continue;
			String strValue = child.getFirstChild().getNodeValue();
			if (strValue == null)
			{
				for (child2 = child.getFirstChild(); child2 != null; child2 = child2.getNextSibling())
				{
					if (child2.getNodeType() != 1)
						continue;
					strValue = child2.getFirstChild().getNodeValue();
					break;
				}
			}

			strLine.append(normalizeString(strValue));
			strLine.append(";");
		}

		strLine.deleteCharAt(strLine.length() - 1);

		if (table.m_listFields.size() != 0)
		{
			String[] fields = strLine.toString().split(";");

			for (XMLTempletP.FieldItem field : table.m_listFields)
			{
				if ((field.m_nFieldIndex > 0) && 
						(field.m_nFieldIndex < fields.length))
				{
					strNewRow.append(fields[field.m_nFieldIndex] + ";");
				}
				else {
					strNewRow.append(";");
				}
			}
		}
		strNewRow.append("\n");
    	return strNewRow;
	}

	private StringBuffer parseTableLines3(XMLTempletP.Table table, Node node, StringBuffer strNewRow)
	{
		for (XMLTempletP.FieldItem field : table.m_listFields)
		{
			switch (field.m_nSrcType)
			{
				case 1:
					Node attrib = node.getAttributes().getNamedItem(field.m_strAttributeName);
					if (attrib == null)
					{
						strNewRow.append(";");
					}
					else {
						String sValue = attrib.getFirstChild().getNodeName();
						strNewRow.append(normalizeString(sValue) + ";");
					}
					break;
				case 2:
					NodeList childList = node.getChildNodes();
					for (int i = 0; i < childList.getLength(); i++)
					{
						Node child = childList.item(i);

						if (child.getNodeType() != 1) {
							continue;
						}
						if (!child.getNodeName().equals(field.m_strInnerTagName))
							continue;
						Node firstChild = child.getFirstChild();
						if (firstChild == null)
							break;
						String sValue = firstChild.getNodeValue();
						strNewRow.append(normalizeString(sValue));

						break;
					}

					strNewRow.append(";");
					break;
				case 3:
					NodeList childList1 = node.getChildNodes();
					for (int i = 0; i < childList1.getLength(); i++)
					{
						Node child = childList1.item(i);

						if (child.getNodeType() != 1) {
							continue;
						}
						if (!child.getNodeName().equals(field.m_strInnerTagName))
						{
							continue;
						}
						NamedNodeMap attribMap = child.getAttributes();
						if (attribMap == null)
						{
							continue;
						}
						Node attrib1 = attribMap.getNamedItem(field.m_strAttributeName);
						if (attrib1 == null)
							continue;
						String attribValue = attrib1.getFirstChild().getNodeValue();
						if (!attribValue.equals(field.m_strAttributeValue)) {
							continue;
						}
						String strValue = child.getFirstChild().getNodeValue();
						strNewRow.append(normalizeString(strValue));
						break;
					}
					
					strNewRow.append(";");
					break;
				case 4:
					NodeList childList4 = node.getChildNodes();
					for (int i = 0; i < childList4.getLength(); i++)
					{
						Node child = childList4.item(i);

						if (child.getNodeType() != 1) {
							continue;
						}
						if (!child.getNodeName().equals(field.m_strInnerTagName))
						{
							continue;
						}
						NamedNodeMap attribMap = child.getAttributes();
						if (attribMap == null)
						{
							continue;
						}
						Node attrib1 = attribMap.getNamedItem(field.m_strAttributeName);
						if (attrib1 == null)
							continue;
						String attribColumName = attrib1.getFirstChild().getNodeValue();
						if (!attribColumName.toUpperCase().equals(field.m_strName)) {
							continue;
						}
						Node attrib2 = attribMap.getNamedItem(field.m_strAttributeValue);
						
						String strValue = attrib2.getFirstChild().getNodeValue();
						strNewRow.append(normalizeString(strValue));
						break;
					}
					
					strNewRow.append(";");
					break;
				default:
					strNewRow.append(";");
			}

		}

		strNewRow.append("\n");
		return strNewRow;
	}*/
}


class MySAXHandler extends DefaultHandler {   

	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private CollectObjInfo collectObjInfo = null;
	private ParserObj pObj = null;
	private Distribute distribute = null;
	public MySAXHandler(CollectObjInfo collectInfo,
			ParserObj obj,Distribute disObj)
	{
		pObj = obj;
		collectObjInfo = collectInfo;
		this.distribute = disObj;
	}
	
	
	boolean hasAttribute = false;   
	Attributes attributes = null;   
	
	
	
	public void startDocument() throws SAXException {   
		System.out.println("文档开始打印了");   
	}	   

	public void endDocument() throws SAXException {   
		System.out.println("文档打印结束了");   
	}   

	public void startElement(String uri, String localName, String qName,   
			Attributes attributes) throws SAXException 
	{   

	
		
		XMLTempletP templet = (XMLTempletP)this.collectObjInfo.getParseTemplet();
		for (XMLTempletP.ParseTag parseTag : templet.m_listParseTag)
		{
			
			if(qName.equals(parseTag.m_strTagName))
			{//找到一个表的开始
				for(XMLTempletP.Table table : parseTag.m_listTable)
				{
					pObj.tableName = attributes.getValue(table.m_strAttributeName);
					pObj.m_parseTag = parseTag;
					if(table.m_strTableName.equals(pObj.tableName))
					{
						pObj.CurrentTable = table;
						pObj.disIndex = table.m_nTableIndex;
						
						
					}
				}
			}
			
			//行解析模版
			
			
			
			if(pObj.CurrentTable != null)
			{
				if(qName.equals(pObj.CurrentTable.m_strSubTagName))
				{
					//表示一行的开始
					pObj.blRowStart = true;
					SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String strTime = spformat.format(this.collectObjInfo.getLastCollectTime());
					
					pObj.strNewRow.append(strTime + pObj.CurrentTable.m_strNewFieldSplitSign);
				}
			}
			
			if(pObj.blRowStart)
			{
				for( XMLTempletP.FieldItem field : pObj.CurrentTable.m_listFields)
				{
					if(qName.equals(field.m_strInnerTagName) && 
							attributes.getValue(field.m_strAttributeName).toUpperCase().equals(field.m_strName))
					{
						pObj.strNewRow.append(attributes.getValue(field.m_strAttributeValue) + pObj.CurrentTable.m_strNewFieldSplitSign);
					}
				}
			}
			
		}
	}   

	public void endElement(String uri, String localName, String qName)
			throws SAXException {   

		if(pObj.CurrentTable != null)
		{
			if(qName.equals(pObj.CurrentTable.m_strSubTagName))
			{
				//表示一行的END
				pObj.blRowStart = false;
				
				//数据写入文件
				if(this.distribute.getDisTemplet().stockStyle == 4)
				{
					//以文件方式入库  需要把最后一个逗号去掉
					pObj.strNewRow.deleteCharAt(pObj.strNewRow.length() -1);
				}
				pObj.strNewRow.append("\n");
				if(this.distribute.getDisTemplet().encode.isEmpty())
				{
					this.distribute.DistributeData(pObj.strNewRow.toString().getBytes(), pObj.disIndex);
				}
				else
				{//需要对文件字符编码做转换
					try {
						this.distribute.DistributeData(pObj.strNewRow.toString().getBytes(this.distribute.getDisTemplet().encode), pObj.disIndex);
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						log.error("分发文件编码格式转换异常",e);
					}
				}
				
				pObj.strNewRow = new StringBuffer();
				//pObj.CurrentTable = null;
			}
			if(pObj.m_parseTag != null && qName.equals(pObj.m_parseTag.m_strTagName))
			{//一个表的结束
				pObj.CurrentTable = null;
				pObj.disIndex = -1;
			}
		}

	}   

	public void characters(char[] ch, int start, int length)   
			throws SAXException 
	{   

		//System.out.println(new String(ch, start, length));   
	}	   
}  

class ParserObj
{
	public boolean blRowStart = false;
	
	public XMLTempletP.Table CurrentTable = null;
	
	public String tableName = "";
	
	public StringBuffer strNewRow = new StringBuffer();
	
	public int disIndex = -1;
	
	public XMLTempletP.ParseTag m_parseTag;
}