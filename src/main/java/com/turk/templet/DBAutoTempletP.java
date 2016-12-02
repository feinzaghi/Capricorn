package com.turk.templet;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.config.SystemConfig;
import com.turk.exception.InvalidParameterValueException;
import com.turk.parser.xparser.IllegalTagException;
import com.turk.util.Util;

public class DBAutoTempletP extends AbstractTempletBase
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5818379814705809555L;
	private Map<String, Templet> templets = new LinkedHashMap<String, Templet>();

	public void parseTemp(String tempContent)
    	throws Exception
    {
		if (Util.isNull(tempContent)) {
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();

		String templetFilePath = SystemConfig.getInstance().getTempletPath() + 
			File.separatorChar + tempContent;
		File file = new File(templetFilePath);
		Document doc = builder.parse(file);
		NodeList templetList = doc.getElementsByTagName("templet");
		int templetsSize = templetList.getLength();
		if (templetsSize <= 0)
			return;
		Templet temObj = null;

		for (int i = 0; i < templetsSize; i++)
		{
			Node templet = templetList.item(i);
			temObj = getTemplet(templet);
			this.templets.put(temObj.getTableName(), temObj);
		}
    }

	private Templet getTemplet(Node tNode) throws Exception
	{
		Templet t = new Templet();
		Node idAtrr = tNode.getAttributes().getNamedItem("id");
		Node tableAtrr = tNode.getAttributes().getNamedItem("table");
		Node useAtrr = tNode.getAttributes().getNamedItem("used");

		Node condiAtrr = tNode.getAttributes().getNamedItem("condition");

		Node occurAtrr = tNode.getAttributes().getNamedItem("occur");
		if ((idAtrr == null) || (tableAtrr == null) || (useAtrr == null)) throw new IllegalTagException("templetȱ������");

		int id = Integer.parseInt(idAtrr.getNodeValue());
		if (id < 0) {
			throw new InvalidParameterValueException("id = " + id);
		}
		String tableName = tableAtrr.getNodeValue();
		if (Util.isNull(tableName)) {
			throw new InvalidParameterValueException("table����ֵ����Ϊ��");
		}
		int isUsed = Integer.parseInt(useAtrr.getNodeValue());
		if ((isUsed != 0) && (isUsed != 1)) {
			throw new InvalidParameterValueException("used����ֵ����ȷ��ֻ��Ϊ0��1");
		}
		String condition = null;
		if (condiAtrr != null)
		{
			condition = condiAtrr.getNodeValue();
		}

		int occur = 0;
		if (occurAtrr != null)
		{
			occur = Integer.parseInt(occurAtrr.getNodeValue());
			if ((occur != 0) && (occur != 1)) {
				throw new InvalidParameterValueException("occur����ֵ����ȷ��ֻ��Ϊ0��1");
			}
		}
		String sql = null;
    	Map<Integer, Field> fields = t.getFields();
    	for (Node node = tNode.getFirstChild(); node != null; node = node.getNextSibling())
    	{
    		if (node.getNodeType() != 1)
    			continue;
    		if (node.getNodeName().equalsIgnoreCase("field"))
    		{
    			Field f = getField(node);
    			fields.put(Integer.valueOf(f.getIndex()), f);
    		} else {
    			if (!node.getNodeName().equalsIgnoreCase("sql"))
    				continue;
    			sql = node.getFirstChild().getNodeValue();
    		}
    	}

    	t.setId(id);
    	t.setCondition(condition);
    	t.setSql(sql);
    	t.setUsed(isUsed == 1);
    	t.setOccur(occur == 1);
    	t.setTableName(tableName);
    	return t;
	}

	private Field getField(Node fieldNode)
    	throws Exception
    {
		Field field = new Field();

		Node indexNode = fieldNode.getAttributes().getNamedItem("index");
		Node nameNode = fieldNode.getAttributes().getNamedItem("name");
		Node occurNode = fieldNode.getAttributes().getNamedItem("occur");
		if ((indexNode == null) || (nameNode == null)) {
			throw new IllegalTagException("fieldȱ������");
		}
		int index = Integer.parseInt(indexNode.getNodeValue());
		if (index < 0)
			throw new InvalidParameterValueException("field index = " + index);
		String name = nameNode.getNodeValue();
		if (Util.isNull(name))
			throw new InvalidParameterValueException("field name = " + name);
		if (occurNode != null)
			field.setOccur(occurNode.getNodeValue());
		field.setName(name);
		field.setIndex(index);
		return field;
    }

	public Map<String, Templet> getTemplets()
	{
		return this.templets;
	}

	public void setTemplets(Map<String, Templet> templets)
	{
		this.templets = templets;
	}

	public static void main(String[] args)
	{
		DBAutoTempletP p = new DBAutoTempletP();
		try
		{
			p.parseTemp("dbauto_parse.xml");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public class Field
 	{
		private String name;
		private int index;
    	private String occur;
    	private int colType;
    	private int indexInHead = -1;

    	public Field() {
    	}
    	public int getColType() { return this.colType;
    	}

    	public void setColType(int colType)
    	{
    		this.colType = colType;
    	}

    	public String getName()
    	{
    		return this.name;
    	}

    	public void setName(String name)
    	{
    		this.name = name;
    	}

    	public int getIndex()
    	{
    		return this.index;
    	}

    	public void setIndex(int index)
    	{
    		this.index = index;
    	}

    	public int getIndexInHead()
    	{
    		return this.indexInHead;
    	}

    	public void setIndexInHead(int indexInHead)
    	{
    		this.indexInHead = indexInHead;
    	}

    	public String getOccur()
    	{
    		return this.occur;
    	}

    	public void setOccur(String occur)
    	{
    		this.occur = occur;
    	}
 	}

	public class Templet
	{
		private int id;
	    private String tableName;
	    private boolean isUsed;
	    private boolean isOccur;
	    private String sql;
	    private String condition;
	    private Map<Integer, DBAutoTempletP.Field> fields = new TreeMap<Integer, Field>(new IDComparator());

	    public Templet() {
	    }
	    public int getId() { return this.id;
	    }

	    public void setId(int id)
	    {
	    	this.id = id;
	    }

	    public String getTableName()
	    {
	    	return this.tableName;
	    }

	    public void setTableName(String tableName)
	    {
	    	this.tableName = tableName;
	    }
	    
	    public boolean isUsed()
	    {
	    	return this.isUsed;
	    }

	    public void setUsed(boolean isUsed)
	    {
	    	this.isUsed = isUsed;
	    }

	    public Map<Integer, DBAutoTempletP.Field> getFields()
	    {
	    	return this.fields;
	    }

	    public void setFields(Map<Integer, DBAutoTempletP.Field> fields)
	    {
	    	this.fields = fields;
	    }

	    public String getCondition()
	    {
	    	return this.condition;
	    }

	    public void setCondition(String condition)
	    {
	    	this.condition = condition;
	    }

	    public String getSql()
	    {
	    	return this.sql;
	    }

	    public void setSql(String sql)
	    {
	    	this.sql = sql;
	    }

	    public boolean isOccur()
	    {
	    	return this.isOccur;
	    }
	    
	    public void setOccur(boolean isOccur)
	    {
	    	this.isOccur = isOccur;
	    }
	}
}