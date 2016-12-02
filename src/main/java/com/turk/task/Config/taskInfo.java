package com.turk.task.Config;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class taskInfo {
	
	public HashMap<String,String> TaskInfo = new HashMap(); 
	
	/**
	 * 载入任务信息
	 * @param xmlPath
	 * @throws Exception
	 */
	public void parseTask(String xmlPath)
	    	throws Exception
	    {
			if ((xmlPath == null) || (xmlPath.trim().equals(""))) {
				return;
			}
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = null;

			File file = new File(xmlPath);
			doc = builder.parse(file);
			
			NodeList employees = doc.getChildNodes();
			
			for (int i = 0; i < employees.getLength(); i++) 
			{ 
				Node employee = employees.item(i); 
				
				NodeList employeeInfo = employee.getChildNodes(); 
				
				for (int j = 0; j < employeeInfo.getLength(); j++) 
				{ 
					String name = "";
					String value = "";
					Node node = employeeInfo.item(j); 
					NodeList employeeMeta = node.getChildNodes(); 
					for (int k = 0; k < employeeMeta.getLength(); k++) 
					{ 
						if(employeeMeta.item(k).getNodeName().equals("name"))
						{
							name = employeeMeta.item(k).getTextContent();
						}
						
						if(employeeMeta.item(k).getNodeName().equals("value"))
						{
							value = employeeMeta.item(k).getTextContent();
						}
					}
					
					if(!name.isEmpty() && !TaskInfo.containsKey(name))
					{
						TaskInfo.put(name, value);
					}
				}
			} 
				
			
		}
	
	public void SetTaskInfo(String xmlPath,String name,String value)
			throws Exception
	{
		if ((xmlPath == null) || (xmlPath.trim().equals(""))) {
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		boolean isFind = false;
		
		File file = new File(xmlPath);
		doc = builder.parse(file);
		doc.normalize();
		
		NodeList employees = doc.getChildNodes();
		
		for (int i = 0; i < employees.getLength(); i++) 
		{ 
			Node employee = employees.item(i); 
			
			NodeList employeeInfo = employee.getChildNodes(); 
			
			for (int j = 0; j < employeeInfo.getLength(); j++) 
			{ 
				Node node = employeeInfo.item(j); 
				NodeList employeeMeta = node.getChildNodes(); 
				for (int k = 0; k < employeeMeta.getLength(); k++) 
				{
					Node thisNode = employeeMeta.item(k).getFirstChild();
					if(thisNode==null)
						continue;
					System.out.println(thisNode.getNodeValue());
					if(thisNode.getNodeValue().equals(name))
					{
						isFind = true;
					}
					
					if(thisNode.getParentNode().getNodeName().equals("value") && isFind)
					{
						thisNode.setNodeValue(value);
						//System.out.println(employeeMeta.item(k).getTextContent());
						//System.out.println(thisNode.getNodeValue());
						isFind = false;
					}
					
				}
				
			}
		} 
		
		doc2XmlFile(doc, xmlPath);
			
	}
		
	
	 public static boolean doc2XmlFile(Document document, String filename) {
	        boolean flag = true;
	        try {
	           
	            TransformerFactory tFactory = TransformerFactory.newInstance();
	            Transformer transformer = tFactory.newTransformer();
	           
	            // transformer.setOutputProperty(OutputKeys.ENCODING, "GB2312");
	            DOMSource source = new DOMSource(document);
	            StreamResult result = new StreamResult(new File(filename));
	            transformer.transform(source, result);
	        } catch (Exception ex) {
	            flag = false;
	            ex.printStackTrace();
	        }
	        return flag;
	    }
	
	
	public static void main(String[] args)
	{
		try
		{
			taskInfo info = new taskInfo();
			
			info.parseTask("D:\\Workspaces\\Capricorn\\conf\\Task_20000001.xml");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


}
