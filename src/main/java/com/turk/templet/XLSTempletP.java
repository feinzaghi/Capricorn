package com.turk.templet;

import com.turk.config.SystemConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * Excel ½âÎöÄ£°æ
 * @author Administrator
 *
 */
public class XLSTempletP extends AbstractTempletBase
{
	public Map<Integer, SheetInfo> m_mapSheet = new HashMap();

	public void parseTemp(String TempContent) throws Exception
	{
		if ((TempContent == null) || (TempContent.trim().equals(""))) {
			return;
		}
	    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder builder = factory.newDocumentBuilder();
	    Document doc = null;

	    String TempletFilePath = SystemConfig.getInstance().getTempletPath();
	    TempletFilePath = TempletFilePath + File.separatorChar + TempContent;
	    File file = new File(TempletFilePath);
	    doc = builder.parse(file);

	    NodeList nl = doc.getElementsByTagName("SHEET");
	    for (int i = 0; i < nl.getLength(); i++)
	    {
	    	SheetInfo shtInfo = new SheetInfo();
	    	int idx = Integer.parseInt(doc.getElementsByTagName("INDEX").item(i).getFirstChild().getNodeValue());

	    	shtInfo.m_strSheetName = doc.getElementsByTagName("SHEETNAME").item(i).getFirstChild().getNodeValue();
	    	if (doc.getElementsByTagName("HASTITLE").item(i).getFirstChild().getNodeValue().equals("0"))
	    		shtInfo.m_bHasTitle = false;
	    	else {
	    		shtInfo.m_bHasTitle = true;
	    	}
	    	this.m_mapSheet.put(Integer.valueOf(idx), shtInfo);
	    }
	}

	public class SheetInfo
	{
	    public String m_strSheetName;
	    public boolean m_bHasTitle;
	
	    public SheetInfo()
	    {
	    }
	}
}