package com.turk.templet;


import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.exception.InvalidParameterValueException;
import com.turk.templet.Table.Column;
import com.turk.config.SystemConfig;
import com.turk.distributor.DistributeTemplet;
import com.turk.util.Util;

public class GenericSectionHeadD extends DistributeTemplet
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4280287068358117265L;
	private Map<Integer, Templet> templets = new HashMap<Integer, Templet>();

	public Templet getTemplet(int id)
	{
		return (Templet)this.templets.get(Integer.valueOf(id));
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
		NodeList templetsList = doc.getElementsByTagName("templet");
		int templetsSize = templetsList.getLength();
		if (templetsSize <= 0) {
			return;
		}
		for (int i = 0; i < templetsSize; i++)
		{
			Templet temObj = new Templet();
			Node templet = templetsList.item(i);

			int id = Integer.parseInt(templet.getAttributes().getNamedItem("id").getNodeValue());
			if (id < 0)
				throw new InvalidParameterValueException("templet id = " + id);
			temObj.setId(id);
			Map<Integer, Table> tables = temObj.getTables();
			for (Node node = templet.getFirstChild(); node != null; node = node.getNextSibling())
			{
				if (node.getNodeType() != 1)
					continue;
				String nodeName = node.getNodeName();
				if (!nodeName.equalsIgnoreCase("table"))
					continue;
				Table table = getTable(node);
				tables.put(Integer.valueOf(table.getId()), table);
			}
			this.templets.put(Integer.valueOf(id), temObj);
		}
    }

	private Table getTable(Node tableNode)
    	throws Exception
    {
		Table table = new Table();
		int id = Integer.parseInt(tableNode.getAttributes().getNamedItem("id").getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("table id = " + id);
		String name = tableNode.getAttributes().getNamedItem("name").getNodeValue();
		String splitSign = tableNode.getAttributes().getNamedItem("split").getNodeValue();
		Map<Integer, Column> columns = table.getColumns();
		for (Node node = tableNode.getFirstChild(); node != null; node = node.getNextSibling())
		{	
			if (node.getNodeType() != 1)
				continue;
			if (!node.getNodeName().equalsIgnoreCase("column"))
				continue;
			Table.Column c = getColumn(node);
			columns.put(Integer.valueOf(c.getIndex()), c);
		}

		table.setId(id);
		table.setName(name);
		table.setSplitSign(splitSign);
		return table;
    }

	private Table.Column getColumn(Node columnNode)
    	throws Exception
    {
		Table tmp11_8 = new Table(); 
		tmp11_8.getClass(); 
		Table.Column c = tmp11_8.new Column();
		String name = columnNode.getAttributes().getNamedItem("name").getNodeValue();
		int index = Integer.parseInt(columnNode.getAttributes().getNamedItem("index").getNodeValue());
		if (index < 0)
			throw new InvalidParameterValueException("column index = " + index);
		int type = -1;
		Node tNode = columnNode.getAttributes().getNamedItem("type");
		if (tNode != null)
		{	
			type = Integer.parseInt(tNode.getNodeValue());
		}
		Node fNode = columnNode.getAttributes().getNamedItem("format");
		if (fNode != null)
			c.setFormat(fNode.getNodeValue());
		c.setIndex(index);
		c.setName(name);
		c.setType(type);
		return c;
    }

	public static void main(String[] args)
	{
		GenericSectionHeadD d = new GenericSectionHeadD();
		try
		{
			d.parseTemp("bell_pm_alt_b10_gbl_dist.xml");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public class Templet
	{
		private int id;
		private Map<Integer, Table> tables = new HashMap<Integer, Table>();

		public Templet() {
		}
		public int getId() { return this.id;
		}

		public void setId(int id)
		{
			this.id = id;
		}

		public Map<Integer, Table> getTables()
		{
			return this.tables;
		}

		public void setTables(Map<Integer, Table> tables)
		{
			this.tables = tables;
		}
	}
}