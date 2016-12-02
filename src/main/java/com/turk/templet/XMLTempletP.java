package com.turk.templet;

import com.turk.config.SystemConfig;

import java.io.File;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 	XML Ä£°æ
 * @author Administrator
 *
 */
public class XMLTempletP extends AbstractTempletBase
{
	public Vector<ParseTag> m_listParseTag = new Vector();

	public void parseTemp(String templetFileName) throws Exception
	{
		if ((templetFileName == null) || (templetFileName.trim().equals(""))) {
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + templetFileName;
		File file = new File(TempletFilePath);
		doc = builder.parse(file);

		NodeList nl = doc.getElementsByTagName("PARSE_TAG");
		for (int i = 0; i < nl.getLength(); i++)
		{
			ParseTag parseTag = new ParseTag();
			for (Node childNode = nl.item(i).getFirstChild(); childNode != null; childNode = childNode.getNextSibling())
			{
				if (childNode.getNodeType() != 1)
				{
					continue;
				}
				if (childNode.getNodeName().toUpperCase().equals("SRC_TAGNAME"))
				{
					parseTag.m_strTagName = childNode.getFirstChild().getNodeValue();
				}
				else if(childNode.getNodeName().toUpperCase().equals("SRC_ATTRIB_NAME"))
				{
					parseTag.m_strAttriName = childNode.getFirstChild().getNodeValue();
				}
				else
				{
					if (!childNode.getNodeName().toUpperCase().equals("TABLE"))
					{
						continue;
					}

					Table table = new Table();
					parseTag.m_listTable.add(table);
					fillTable(table, childNode);
				}
			}

			this.m_listParseTag.add(parseTag);
		}
	}

	private void fillTable(Table table, Node tableNode)
	{
		NodeList childNodeList = tableNode.getChildNodes();
		try
		{
			for (int i = 0; i < childNodeList.getLength(); i++)
			{
				Node childNode = childNodeList.item(i);

				if (childNode.getNodeType() != 1) {
					continue;
				}
				String strName = childNode.getNodeName();
				if (strName.equals("TABLE_INDEX"))
				{
					table.m_nTableIndex = Integer.parseInt(childNode.getFirstChild().getNodeValue());
				}
				else if (strName.equals("TABLE_NAME"))
				{
					table.m_strTableName = childNode.getFirstChild().getNodeValue();
				}
				else if (strName.equals("GEN_COLUMN_TYPE"))
				{
					table.m_nGenColumnType = Integer.parseInt(childNode.getFirstChild().getNodeValue());
				}
				else if (strName.equals("SRC_ID_TYPE"))
				{
					table.m_nSrcIDType = Integer.parseInt(childNode.getFirstChild().getNodeValue());
				}
				else if ((strName.equals("SRC_ID_ATTRIB_NAME")) && 
						(childNode.getFirstChild() != null))
				{
					table.m_strAttributeName = childNode.getFirstChild().getNodeValue();
				}
				else if ((strName.equals("SRC_ID_ATTRIB_VALUE")) && 
						(childNode.getFirstChild() != null))
				{
					table.m_strAttributeValue = childNode.getFirstChild().getNodeValue();
				}
				else if ((strName.equals("NEWFIELDSPLITSIGN")) && 
						(childNode.getFirstChild() != null))
				{
					table.m_strNewFieldSplitSign = childNode.getFirstChild().getNodeValue();
				}
				else if ((strName.equals("SUB_ELEMENT_TAGNAME")) && 
						(childNode.getFirstChild() != null))
				{
					table.m_strSubTagName = childNode.getFirstChild().getNodeValue();
				} else {
					if ((!strName.equals("COLUMNS")) || 
							(childNode.getFirstChild() == null))
						continue;
					fillColumns(table.m_listFields, childNode);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private void fillColumns(Vector<FieldItem> fields, Node columnsNode)
	{
		NodeList childNodeList = columnsNode.getChildNodes();
		try
		{
			for (int i = 0; i < childNodeList.getLength(); i++)
			{
				Node childNode = childNodeList.item(i);
				if (childNode.getNodeType() != 1) {
					continue;
				}
				String strName = childNode.getNodeName();
				if (!strName.equals("FIELDITEM"))
					continue;
    		    FieldItem fieldItem = new FieldItem();
    		    fields.add(fieldItem);

    		    NodeList fieldsList = childNode.getChildNodes();
    		    for (int j = 0; j < fieldsList.getLength(); j++)
    		    {
    		    	Node fieldNode = fieldsList.item(j);

    		    	if (fieldNode.getNodeType() != 1) {
    		    		continue;
    		    	}
    		    	String strFieldElementName = fieldNode.getNodeName();
    		    	if (strFieldElementName.equals("FIELDNAME"))
    		    	{
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue()!=null)
    		    			fieldItem.m_strName = fieldNode.getFirstChild().getNodeValue();
    		    		else
    		    			fieldItem.m_strName = "";
    		    	}
    		    	else if (strFieldElementName.equals("COLUMN_SRC_TYPE"))
    		    	{
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue()!=null)
    		    			fieldItem.m_nSrcType = Integer.parseInt(fieldNode.getFirstChild().getNodeValue());
    		    		else
    		    			fieldItem.m_nSrcType = -1;
    		    	}
    		    	else if (strFieldElementName.equals("FIELDINDEX"))
    		    	{
    		    		fieldItem.m_nFieldIndex = Integer.parseInt(fieldNode.getFirstChild().getNodeValue());
    		    	}
    		    	else if (strFieldElementName.equals("FIELDTYPE"))
    		    	{
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue()!=null)
    		    			fieldItem.m_strType = fieldNode.getFirstChild().getNodeValue();
    		    		else
    		    			fieldItem.m_strType = "";
    		    	}
    		    	else if (strFieldElementName.equals("DATEFORMAT"))
    		    	{
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue() != null)
    		    			fieldItem.m_strDateFormat = fieldNode.getFirstChild().getNodeValue();
    		    		else
    		    			fieldItem.m_strDateFormat = "";
    		    	}
    		    	else if (strFieldElementName.equals("INNER_ELEMENT_TAGNAME"))
    		    	{
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue()!=null)
    		    			fieldItem.m_strInnerTagName = fieldNode.getFirstChild().getNodeValue();
    		    		else
    		    			fieldItem.m_strInnerTagName = "";
    		    	}
    		    	else if (strFieldElementName.equals("ELEMENT_ATTRIBUTE"))
    		    	{
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue()!=null)
    		    			fieldItem.m_strAttributeName = fieldNode.getFirstChild().getNodeValue();
    		    		else
    		    			fieldItem.m_strAttributeName = "";
    		    	} else {
    		    		if (!strFieldElementName.equals("ATTRIBUTE_VALUE"))
    		    			continue;
    		    		if(fieldNode.getFirstChild()!=null && fieldNode.getFirstChild().getNodeValue()!=null)
    		    			fieldItem.m_strAttributeValue = fieldNode.getFirstChild().getNodeValue();
    		    		else
    		    			fieldItem.m_strAttributeValue = "";
    		    	}
    		    }
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public class FieldItem
	{
		public String m_strName;
		public String m_strType;
    	public String m_strDateFormat;
    	public int m_nFieldIndex;
    	public int m_nSrcType;
    	public String m_strInnerTagName;
    	public String m_strAttributeName;
    	public String m_strAttributeValue;

    	public FieldItem()
    	{
    	}
	}

	public class ParseTag
	{
		public String m_strTagName;
		public String m_strAttriName;
		public Vector<XMLTempletP.Table> m_listTable = new Vector();

		public ParseTag()
		{
		}
	}

	public class Table
	{
		public int m_nTableIndex;
		public int m_nGenColumnType;
		public int m_nSrcIDType;
		public String m_strTableName;
		public String m_strAttributeName;
		public String m_strAttributeValue;
		public String m_strSubTagName;
		public String m_strNewFieldSplitSign;
		public Vector<XMLTempletP.FieldItem> m_listFields = new Vector();

		public Table()
		{
		}
	}
}