package com.turk.templet;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.config.SystemConfig;
import com.turk.exception.InvalidParameterValueException;
import com.turk.parser.xparser.IllegalTagException;
import com.turk.util.Util;

public class DBAutoTempletP2 extends AbstractTempletBase
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1999881974322674999L;

	private Map<String, Templet> templets = new LinkedHashMap<String, Templet>();

	private Map<String, Map<String, String>> mappingfields = new HashMap<String, Map<String, String>>();

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
	    	this.templets.put(temObj.getFromTableName(), temObj);
	    }
    }

	private Templet getTemplet(Node tNode) throws Exception
	{
	    Templet t = new Templet();
	    Node idAtrr = tNode.getAttributes().getNamedItem("id");
	    Node tableAtrr = tNode.getAttributes().getNamedItem("fromTable");
	    Node destableAtrr = tNode.getAttributes().getNamedItem("toTable");
	    Node useAtrr = tNode.getAttributes().getNamedItem("used");

	    Node occurAtrr = tNode.getAttributes().getNamedItem("occur");

	    Node condiAtrr = tNode.getAttributes().getNamedItem("condition");
	    if ((idAtrr == null) || (tableAtrr == null) || (destableAtrr == null) || 
	    		(useAtrr == null))
	    {
	    	throw new IllegalTagException("templet缺少属性");
	    }
	    int id = Integer.parseInt(idAtrr.getNodeValue());
	    if (id < 0) {
	    	throw new InvalidParameterValueException("id = " + id);
	    }
	    String tableName = tableAtrr.getNodeValue();
	    if (Util.isNull(tableName)) {
	    	throw new InvalidParameterValueException("table属性值不能为空");
	    }
	    String destable = destableAtrr.getNodeValue();
	    if (Util.isNull(destable)) {
	    	throw new InvalidParameterValueException("destable属性值不能为空");
	    }

	    int isUsed = Integer.parseInt(useAtrr.getNodeValue());
	    if ((isUsed != 0) && (isUsed != 1)) {
	    	throw new InvalidParameterValueException("used属性值不正确，只能为0或1");
	    }
	    int occurAtrrValue = 0;

	    if (occurAtrr != null)
	    {
	    	if (Util.isNotNull(occurAtrr.getNodeValue()))
	    	{
	    		occurAtrrValue = occurAtrr.getNodeValue().trim().equals("1") ? 1 : 0;
	    	}
	    }

	    String condition = null;
	    if (condiAtrr != null)
	    {
	    	condition = condiAtrr.getNodeValue();
	    }

	    String sql = null;

	    if (tNode.getFirstChild() != null)
	    {
	    	Map<String, String> cmap = new HashMap<String, String>();
	    	for (Node node = tNode.getFirstChild(); node != null; node = node.getNextSibling())
	    	{
	    		if (node.getNodeType() != 1)
	    			continue;
	    		if (node.getNodeName().equalsIgnoreCase("mapping"))
	    		{
	    			NodeList nList = node.getChildNodes();
	    			for (int i = 0; i < nList.getLength(); i++)
	    			{
	    				Node n = nList.item(i);
	    				if (n.getNodeType() != 1)
	    					continue;
	    				if (!n.getNodeName().equalsIgnoreCase("column"))
	    					continue;
	    				Node nameNode = n.getAttributes().getNamedItem("src");
			            Node mappingField = n.getAttributes().getNamedItem("dest");
			            String name = nameNode.getNodeValue();
			            String mappingValue = mappingField.getNodeValue();
			            if (Util.isNull(name))
			            	throw new InvalidParameterValueException("src name = " + name);
			            if (Util.isNull(mappingValue)) {
			            	throw new InvalidParameterValueException("desc name = " + 
			            			mappingValue);
			            }
			            cmap.put(mappingValue, name);
	    			}
	    		}
	    		else
	    		{
	    			if (!node.getNodeName().equalsIgnoreCase("sql"))
	    				continue;
	    			sql = node.getTextContent();
	    		}
	    	}
	    	this.mappingfields.put(destable, cmap);
	    }
	    t.setId(id);
	    t.setCondition(condition);
	    t.setUsed(isUsed == 1);
	    t.setSql(sql);
	    t.setOccur(occurAtrrValue == 1);
	    t.setFromTableName(tableName);
	    t.setDestTableName(destable);
	    return t;
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
		DBAutoTempletP2 p = new DBAutoTempletP2();
		try
		{
			p.parseTemp("db_v9r11_clt_cm_hw_parse_sybase.xml");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public Map<String, Map<String, String>> getMappingfields()
	{
		return this.mappingfields;
	}

	public void setMappingfields(Map<String, Map<String, String>> mappingfields)
	{
		this.mappingfields = mappingfields;
	}

	public class Templet
	{
	    private int id;
	    private String fromTableName;
	    private boolean isUsed;
	    private String condition;
	    private String toTableName;
	    private String sql;
	    private boolean occur;

	    public Templet()
	    {
	    }

	    public boolean isOccur()
	    {
	    	return this.occur;
	    }

	    public void setOccur(boolean occur)
	    {
	    	this.occur = occur;
	    }

	    public String getSql()
	    {
	    	return this.sql;
	    }

	    public void setSql(String sql)
	    {
	    	this.sql = sql;
	    }

	    public int getId()
	    {
	    	return this.id;
	    }

	    public void setId(int id)
	    {
	    	this.id = id;
	    }

	    public String getFromTableName()
	    {
	    	return this.fromTableName;
	    }

	    public void setFromTableName(String tbName)
	    {
	    	this.fromTableName = tbName;
	    }

	    public boolean isUsed()
	    {
	    	return this.isUsed;
	    }

	    public void setUsed(boolean isUsed)
	    {
	    	this.isUsed = isUsed;
	    }

	    public String getCondition()
	    {
	    	return this.condition;
	    }

	    public void setCondition(String condition)
   		{
	    	this.condition = condition;
   		}

	    public String getDestTableName()
	    {
	    	return this.toTableName;
	    }

	    public void setDestTableName(String tbName)
	    {
	    	this.toTableName = tbName;
	    }	
	}
}