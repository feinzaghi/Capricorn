package com.turk.templet;

import com.turk.Config.SystemConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.exception.InvalidParameterValueException;
import com.turk.parser.xparser.IllegalTagException;

import com.turk.util.Util;

public class GenericSectionHeadP extends AbstractTempletBase
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7706573381539684642L;
	private Map<String, Templet> templets = new HashMap<String, Templet>();

	public Map<String, Templet> getTemplets()
	{
		return this.templets;
	}

	public void parseTemp(String tempContent)
    	throws Exception
    {
		if (Util.isNull(tempContent)) {
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();

		String templetFilePath = SystemConfig.getInstance().getTempletPath() + 
			File.separatorChar + tempContent.trim();
		File file = new File(templetFilePath);
		Document doc = builder.parse(file);
		NodeList templetList = doc.getElementsByTagName("templet");
		int templetsSize = templetList.getLength();
		if (templetsSize <= 0)
			return;
		Templet temObj = null;

		for (int i = 0; i < templetsSize; i++)
		{
			temObj = new Templet();
			Node templet = templetList.item(i);

			int id = Integer.parseInt(templet.getAttributes().getNamedItem("id").getNodeValue());
			if (id < 0) {
				throw new InvalidParameterValueException("templet id = " + id);
			}

			Node fileAtrr = templet.getAttributes().getNamedItem("file");
			if (fileAtrr != null)
			{
				String fileName = fileAtrr.getNodeValue();
				if (Util.isNull(fileName))
					throw new InvalidParameterValueException("file属性值不能为空");
				temObj.setFileName(fileName);
			}
			else {
				throw new IllegalTagException("缺少file属性");
			}
			Map<Integer, DS> dsMap = new HashMap<Integer, DS>();
			for (Node node = templet.getFirstChild(); node != null; node = node.getNextSibling())
			{
				if (node.getNodeType() != 1)
					continue;
				String nodeName = node.getNodeName();
				if (nodeName.equalsIgnoreCase("public"))
				{
					Public publicObj = getPublic(node);
					temObj.setPublicElement(publicObj);
				} else {
					if (!nodeName.equalsIgnoreCase("ds"))
						continue;
					DS dsObj = getDS(node);
					dsMap.put(Integer.valueOf(dsObj.getId()), dsObj);
				}
			}

			temObj.setDsMap(dsMap);
			temObj.setId(id);
			this.templets.put(temObj.getFileName(), temObj);
		}
    }

	private Public getPublic(Node publicNode)
    	throws Exception
    {
		Public pub = new Public();
		for (Node node = publicNode.getFirstChild(); node != null; node = node.getNextSibling())
		{
			if (node.getNodeType() != 1)
				continue;
			String nodeName = node.getNodeName();
			if (nodeName.equalsIgnoreCase("startSign"))
			{
				pub.setStartSign(node.getFirstChild().getNodeValue());
			}
			else if (nodeName.equalsIgnoreCase("endSign"))
			{
				pub.setEndSign(node.getFirstChild().getNodeValue());
			} else {
				if (!nodeName.equalsIgnoreCase("fields"))
					continue;
				Fields fields = getFields(node);
				pub.setFields(fields);
			}
		}

		return pub;
    }

	private DS getDS(Node dsNode)
    	throws Exception
    {
		DS ds = new DS();
		int dsId = Integer.parseInt(dsNode.getAttributes().getNamedItem("id").getNodeValue());
		if (dsId < 0) {
			throw new InvalidParameterValueException("ds id = " + dsId);
		}
		ds.setId(dsId);

		for (Node node = dsNode.getFirstChild(); node != null; node = node.getNextSibling())
		{
			if (node.getNodeType() != 1)
				continue;
			String nodeName = node.getNodeName();
			if (nodeName.equalsIgnoreCase("meta"))
			{
				ds.setMeta(getMeta(node));
			} else {
				if (!nodeName.equalsIgnoreCase("fields"))
					continue;
				Fields fields = getFields(node);
				ds.setFields(fields);

				Collection<Field> cFields = fields.getFields().values();
				if (cFields.isEmpty())
					continue;
				List<String> ro = new ArrayList<String>();
				for (Field field : cFields)
				{
					String occur = field.getOccur();
					if ((occur == null) || 
							(!occur.equalsIgnoreCase("required")))
						continue;
					ro.add(field.getName());
				}

				ds.setRequiredOccur(ro);
			}
		}
		return ds;
    }

	private Meta getMeta(Node metaNode)
	{
		Meta meta = new Meta();
		for (Node node = metaNode.getFirstChild(); node != null; node = node.getNextSibling())
		{
			if (node.getNodeType() != 1)
				continue;
			String nodeName = node.getNodeName();
			if (nodeName.equalsIgnoreCase("endSign"))
			{
				String endSign = node.getFirstChild().getNodeValue();
				meta.setEndSign(endSign);
			}
			else if (nodeName.equalsIgnoreCase("head"))
			{
				String splitSign = node.getAttributes().getNamedItem("splitSign").getNodeValue();
				meta.setHeadSplitSign(splitSign);
			} else {
				if (!nodeName.equalsIgnoreCase("startSign"))
					continue;
				String startSign = node.getFirstChild().getNodeValue();
				meta.setStartSign(startSign);
			}
		}

		return meta;
	}

	private Fields getFields(Node fieldsNode)
    	throws Exception
    {
		Fields fields = new Fields();
		Node splitNode = fieldsNode.getAttributes().getNamedItem("splitSign");
		if (splitNode != null)
		{
			String splitSign = splitNode.getNodeValue();
			fields.setSplitSign(splitSign);
		}

		Map<Integer, Field> fieldsMap = fields.getFields();
		for (Node node = fieldsNode.getFirstChild(); node != null; node = node.getNextSibling())
		{
			if (node.getNodeType() != 1)
				continue;
			if (!node.getNodeName().equalsIgnoreCase("field"))
				continue;
			Field f = getField(node);
			fieldsMap.put(Integer.valueOf(f.getIndex()), f);
		}

		return fields;
    }

	private Field getField(Node fieldNode)
    	throws Exception
    {
		Field field = new Field();

		Node indexNode = fieldNode.getAttributes().getNamedItem("index");
		if (indexNode == null) {
			throw new IllegalTagException("缺少index属性");
		}
		int index = Integer.parseInt(indexNode.getNodeValue());
		if (index < 0) {
			throw new InvalidParameterValueException("field index = " + index);
		}
		field.setIndex(index);

		Node nameNode = fieldNode.getAttributes().getNamedItem("name");
		if (nameNode != null)
		{
			String name = nameNode.getNodeValue();
			field.setName(name);
		}
		Node occurNode = fieldNode.getAttributes().getNamedItem("occur");
		if (occurNode != null)
		{
			String occur = occurNode.getNodeValue();
			field.setOccur(occur);
		}
		for (Node node = fieldNode.getFirstChild(); node != null; node = node.getNextSibling())
		{
			if (node.getNodeType() != 1)
				continue;
			String nodeName = node.getNodeName();
			if (nodeName.equalsIgnoreCase("startSign"))
			{
				field.setStartSign(node.getFirstChild().getNodeValue());
			} else {
				if (!nodeName.equalsIgnoreCase("endSign"))
					continue;
				field.setEndSign(node.getFirstChild().getNodeValue());
			}

		}

		return field;
    }

	public static void main(String[] args)
	{
		GenericSectionHeadP p = new GenericSectionHeadP();
		try
		{
			p.parseTemp("bell_pm_alt_b10_gbl_parse.xml");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public class DS
	{
	    private int id;
	    private GenericSectionHeadP.Meta meta;
	    private GenericSectionHeadP.Fields fields;
	    private List<String> requiredOccur;

	    public DS()
	    {
	    }

	    public int getId()
	    {
	    	return this.id;
	    }

	    public void setId(int id)
	    {
	    	this.id = id;
	    }

	    public GenericSectionHeadP.Meta getMeta()
	    {
	    	return this.meta;
	    }

	    public void setMeta(GenericSectionHeadP.Meta meta)
	    {
	    	this.meta = meta;
	    }

	    public GenericSectionHeadP.Fields getFields()
	    {
	    	return this.fields;
	    }

	    public void setFields(GenericSectionHeadP.Fields fields)
	    {
	    	this.fields = fields;
	    }

	    public List<String> getRequiredOccur()
	    {
	    	return this.requiredOccur;
	    }

	    public void setRequiredOccur(List<String> requiredOccur)
	    {
	    	this.requiredOccur = requiredOccur;
	    }
	}

	public class Field
	{
	    private String name;
	    private int index;
	    private String occur;
	    private String startSign;
	    private String endSign;
	    private String value;
	    private int indexInHead = -1;

	    public Field() {
    	}
    
	    public String getName() { return this.name;
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

	    public String getStartSign()
	    {
	    	return this.startSign;
	    }

	    public void setStartSign(String startSign)
	    {
	    	this.startSign = startSign;
	    }

	    public String getEndSign()
	    {
	    	return this.endSign;
	    }

	    public void setEndSign(String endSign)
	    {
	    	this.endSign = endSign;
	    }

	    public String getValue()
	    {
	    	return this.value;
	    }

	    public void setValue(String value)
	    {	
	    	this.value = value;
	    }

	    public String getOccur()
	    {
	    	return this.occur;
	    }

	    public void setOccur(String occur)
	    {
	    	this.occur = occur;
	    }

	    public int getIndexInHead()
	    {
	    	return this.indexInHead;
	    }

	    public void setIndexInHead(int indexInHead)
	    {
	    	this.indexInHead = indexInHead;
	    }

	    public String toString()
	    {
	    	return "Field [index=" + this.index + ", name=" + this.name + ", value=" + 
	    		this.value + "]";
	    }
	}

	public class Fields
	{
		private Map<Integer, GenericSectionHeadP.Field> fields = new TreeMap<Integer, Field>(new IDComparator());
		private String splitSign;

		public Fields()
		{
		}

		public Map<Integer, GenericSectionHeadP.Field> getFields()
		{
			return this.fields;
		}

		public void setFields(Map<Integer, GenericSectionHeadP.Field> fields)
		{
			this.fields = fields;
		}

		public String getSplitSign()
		{
			return this.splitSign;
		}

		public void setSplitSign(String splitSign)
		{
			this.splitSign = splitSign;
		}
  	}

	public class Meta
	{
		private String startSign;
		private String endSign;
    	private String headSplitSign;

	    public Meta()
	    {
	    }

	    public String getStartSign()
	    {
	    	return this.startSign;
	    }

	    public void setStartSign(String startSign)
	    {
	    	this.startSign = startSign;
	    }

	    public String getEndSign()
	    {
	    	return this.endSign;
	    }

	    public void setEndSign(String endSign)
	    {
	    	this.endSign = endSign;
	    }

	    public String getHeadSplitSign()
	    {
	    	return this.headSplitSign;
	    }

	    public void setHeadSplitSign(String headSplitSign)
	    {
	    	this.headSplitSign = headSplitSign;
	    }
	}

	public class Public
	{
	    private String startSign;
	    private String endSign;
	    private GenericSectionHeadP.Fields fields;

	    public Public()
	    {
	    }

	    public String getStartSign()
	    {
	    	return this.startSign;
	    }

	    public void setStartSign(String startSign)
	    {
	    	this.startSign = startSign;
	    }

	    public String getEndSign()
	    {
	    	return this.endSign;
	    }

	    public void setEndSign(String endSign)
	    {
	    	this.endSign = endSign;
	    }

	    public GenericSectionHeadP.Fields getFields()
	    {
	    	return this.fields;
	    }

	    public void setFields(GenericSectionHeadP.Fields fields)
	    {
	    	this.fields = fields;
	    }
	}

	public class Templet
	{
	    private int id;
	    private String fileName;
	    private GenericSectionHeadP.Public publicElement;
	    private Map<Integer, GenericSectionHeadP.DS> dsMap = new TreeMap<Integer, DS>(new IDComparator());

	    public Templet() {
	    }
	    public int getId() { return this.id;
	    }

	    public void setId(int id)
	    {
	    	this.id = id;
	    }

	    public GenericSectionHeadP.Public getPublicElement()
	    {
	    	return this.publicElement;
	    }

	    public void setPublicElement(GenericSectionHeadP.Public publicElement)
	    {
	    	this.publicElement = publicElement;
	    }

	    public Map<Integer, GenericSectionHeadP.DS> getDsMap()
	    {
	    	return this.dsMap;
	    }

	    public void setDsMap(Map<Integer, GenericSectionHeadP.DS> dsMap)
	    {
	    	this.dsMap = dsMap;
	    }

	    public String getFileName()
	    {
	    	return this.fileName;
	    }

	    public void setFileName(String fileName)
	    {
	    	this.fileName = fileName;
	    }
	}
}