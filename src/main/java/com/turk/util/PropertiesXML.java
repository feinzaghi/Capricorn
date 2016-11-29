package com.turk.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import com.turk.Config.SystemConfigException;

public class PropertiesXML
{
	private static final long serialVersionUID = -4434706683171361034L;
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();
	private Document document;
	private String xmlLocation;
	private Map<String, String> propertiesMap = new HashMap();

	public PropertiesXML(String xmlLocation)
    	throws SystemConfigException
    {
		this.xmlLocation = xmlLocation;
		loadXML();
    }

	public void setProperty(String name, String value)
    	throws SystemConfigException
    {
		Node selectedNode = select(propertyToXPath(name));
		if (selectedNode == null)
		{
			throw new SystemConfigException("此属性不存在:" + name);
		}

		selectedNode.setText(value);
		write();
		this.propertiesMap.put(name, value);
    }

  	/**
  	 * 获取XML NODE属性
  	 * @param name
  	 * @return
  	 */
	public String getProperty(String name)
	{
		if (this.propertiesMap.containsKey(name)) 
			return (String)this.propertiesMap.get(name);

		String xpath = propertyToXPath(name);
		Node selectedNode = null;
		try {
			selectedNode = select(xpath);
		} catch (SystemConfigException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (selectedNode == null) 
			return null;
		String value = selectedNode.getText();
		this.propertiesMap.put(name, value);

		return value;
	}

	public List<String> getPropertyes(String name)
	{
		List tmp = new ArrayList();

		String xpath = propertyToXPath(name);

		List<Element> es = this.document.selectNodes(xpath);
		if ((es == null) || (es.size() == 0)) 
			return tmp;

		for (Element e : es)
		{
			tmp.add(e.getTextTrim());
		}

		return tmp;
	}

	public List<Element> getChildElementsByPropertyName(String propertyName)
	{
		String xpath = propertyToXPath(propertyName);

		Node selectedNode = null;
		try {
			selectedNode = select(xpath);
		} catch (SystemConfigException e) {
		// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (selectedNode == null) return null;

		return ((Element)selectedNode).elements();
	}

	private Node select(String xpath)
		throws SystemConfigException
	{
		try
	    {
			Node selectedNode = this.document.selectSingleNode(xpath);
			if (selectedNode == null)
			{
				selectedNode = this.document.selectSingleNode(changeToAttribute(xpath));
				if (selectedNode == null) return null;
			}
			return selectedNode;
	    }
		catch (Exception e)
	    {
	      logger.error(e.getMessage());
	      throw new SystemConfigException("载入xml文件时发生异常", e);
	    }
	}

	private void loadXML()
    	throws SystemConfigException
    {
		SAXReader reader = new SAXReader();
		try
		{
			this.document = reader.read(new FileInputStream(this.xmlLocation));
		}
		catch (Exception e)
		{
			logger.error(e.getMessage());
			throw new SystemConfigException("载入xml文件时发生异常", e);
		}
    }

	private String propertyToXPath(String property)
	{
		String xpath = property.replace('.', '/');
		return "/" + xpath;
	}

	private String changeToAttribute(String xpath)
	{
		int index = xpath.lastIndexOf('/');
		index++; return new StringBuilder(xpath).insert(index, "@").toString();
	}

	private void write()
    	throws SystemConfigException
    {
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("utf-8");

		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		XMLWriter writer = null;
		try
		{
			fos = new FileOutputStream(this.xmlLocation);
			osw = new OutputStreamWriter(fos, "utf-8");

			writer = new XMLWriter(osw, format);
			writer.write(this.document);
		}
		catch (Exception e)
		{
			logger.error("写入xml时出现异常");
			throw new SystemConfigException(e.getMessage(), e);
		}
		finally
		{
			try
			{
				writer.flush();
				writer.close();
				osw.flush();
				osw.close();
				fos.flush();
				fos.close();
			}
			catch (Exception localException1)
			{
			}
		}
    }
}