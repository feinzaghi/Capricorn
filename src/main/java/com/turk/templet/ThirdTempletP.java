package com.turk.templet;

import com.turk.Config.SystemConfig;
import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class ThirdTempletP extends AbstractTempletBase
{
  public int nAppType = 1;
  public int nLocateJava = 1;
  public int nLocate = 1;
  public int nMRSource = 0;

  public int ncontextappendtype = 0;
  public int nfilenamesplittype = 0;

  public void parseTemp(String TempContent)
    throws Exception
  {
    if ((TempContent == null) || (TempContent.trim().equals(""))) {
      return;
    }
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    builder = factory.newDocumentBuilder();
    Document doc = null;

    String TempletFilePath = SystemConfig.getInstance().getTempletPath();
    TempletFilePath = TempletFilePath + File.separatorChar + 
      TempContent;
    File file = new File(TempletFilePath);
    doc = builder.parse(file);

    NodeList pn = doc.getElementsByTagName("PUBLIC");
    if (pn.getLength() >= 1)
    {
      try
      {
        this.nAppType = Integer.parseInt(doc.getElementsByTagName("APPTYPE").item(0).getFirstChild().getNodeValue());

        this.nLocateJava = Integer.parseInt(doc.getElementsByTagName("LOCATEJAVA").item(0).getFirstChild().getNodeValue());
        this.nLocate = Integer.parseInt(doc.getElementsByTagName("LOCATE").item(0).getFirstChild().getNodeValue());
        this.nMRSource = Integer.parseInt(doc.getElementsByTagName("MRSOURCE").item(0).getFirstChild().getNodeValue());
        this.ncontextappendtype = Integer.parseInt(doc.getElementsByTagName("CONTEXTAPPENDTYPE").item(0).getFirstChild().getNodeValue());
      }
      catch (Exception localException)
      {
      }

      try
      {
        this.nfilenamesplittype = Integer.parseInt(doc.getElementsByTagName("FILENAMESPLITTYPE").item(0).getFirstChild().getNodeValue());
      }
      catch (Exception localException1)
      {
      }
    }
  }

  public int getNcontextappendtype()
  {
    return this.ncontextappendtype;
  }

  public void setNcontextappendtype(int ncontextappendtype)
  {
    this.ncontextappendtype = ncontextappendtype;
  }

  public int getNfilenamesplittype()
  {
    return this.nfilenamesplittype;
  }

  public void setNfilenamesplittype(int nfilenamesplittype)
  {
    this.nfilenamesplittype = nfilenamesplittype;
  }
}