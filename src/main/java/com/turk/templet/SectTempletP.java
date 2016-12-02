package com.turk.templet;

import com.turk.config.SystemConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SectTempletP extends AbstractTempletBase
{
  public int m_nSectScanType = 0;
  public String m_strSectSplitSign = "";
  public String m_strHeadSectSplitSign = "";
  public String m_strTailSectSplitSign = "";

  public Map<Integer, SectTemplet> m_SectTemplet = new HashMap();

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

    NodeList pn = doc.getElementsByTagName("PUBLIC");
    if (pn.getLength() >= 1)
    {
      if (doc.getElementsByTagName("SECTTYPE").item(0).getFirstChild() == null)
        this.m_nSectScanType = 0;
      else
        this.m_nSectScanType = Integer.parseInt(doc.getElementsByTagName("SECTTYPE").item(0).getFirstChild().getNodeValue());
      if (doc.getElementsByTagName("SECTSIGN").item(0).getFirstChild() == null)
        this.m_strSectSplitSign = "";
      else
        this.m_strSectSplitSign = doc.getElementsByTagName("SECTSIGN").item(0).getFirstChild().getNodeValue();
      this.m_strSectSplitSign = WrapPromptChange(this.m_strSectSplitSign);

      if (doc.getElementsByTagName("HEADSIGN").item(0).getFirstChild() == null)
        this.m_strHeadSectSplitSign = "";
      else
        this.m_strHeadSectSplitSign = doc.getElementsByTagName("HEADSIGN").item(0).getFirstChild().getNodeValue();
      this.m_strHeadSectSplitSign = WrapPromptChange(this.m_strHeadSectSplitSign);

      if (doc.getElementsByTagName("TAILSIGN").item(0).getFirstChild() == null)
        this.m_strTailSectSplitSign = "";
      else
        this.m_strTailSectSplitSign = doc.getElementsByTagName("TAILSIGN").item(0).getFirstChild().getNodeValue();
      this.m_strTailSectSplitSign = WrapPromptChange(this.m_strTailSectSplitSign);
    }
    NodeList Sn = doc.getElementsByTagName("SECTITEM");

    for (int i = 0; i < Sn.getLength(); i++)
    {
      SectTemplet secttemp = new SectTemplet();
      if (doc.getElementsByTagName("SECTINDEX").item(i).getFirstChild() == null)
        secttemp.m_nSectTypeIndex = 0;
      else {
        secttemp.m_nSectTypeIndex = Integer.parseInt(doc.getElementsByTagName("SECTINDEX").item(i).getFirstChild().getNodeValue());
      }
      if (doc.getElementsByTagName("KEYSEARCHTYPE").item(i).getFirstChild() == null)
        secttemp.m_nSectKeySearchType = 0;
      else {
        secttemp.m_nSectKeySearchType = Integer.parseInt(doc.getElementsByTagName("KEYSEARCHTYPE").item(i).getFirstChild().getNodeValue());
      }
      if (doc.getElementsByTagName("KEYWORD").item(i).getFirstChild() == null)
        secttemp.m_strSectKeyWord = "";
      else
        secttemp.m_strSectKeyWord = WrapPromptChange(doc.getElementsByTagName("KEYWORD").item(i).getFirstChild().getNodeValue());
      secttemp.m_strSectKeyWord = WrapPromptChange(secttemp.m_strSectKeyWord);
      try
      {
        if (doc.getElementsByTagName("HEADLINE").item(i).getFirstChild() == null)
          secttemp.headLine = "";
        else
          secttemp.headLine = WrapPromptChange(doc.getElementsByTagName("HEADLINE").item(i).getFirstChild().getNodeValue());
        secttemp.headLine = WrapPromptChange(secttemp.headLine);
      }
      catch (Exception localException)
      {
      }

      if ((doc.getElementsByTagName("COMMONFIELDLIST").item(i) == null) || 
        (doc.getElementsByTagName("COMMONFIELDLIST").item(i).getFirstChild() == null))
        secttemp.m_strCommonFieldList = "";
      else
        secttemp.m_strCommonFieldList = WrapPromptChange(doc.getElementsByTagName("COMMONFIELDLIST").item(i).getFirstChild().getNodeValue());
      secttemp.m_strCommonFieldList = WrapPromptChange(secttemp.m_strCommonFieldList);

      if (doc.getElementsByTagName("NEWSPLITSIGN").item(i).getFirstChild() == null)
        secttemp.m_strNewSplitSign = ";";
      else
        secttemp.m_strNewSplitSign = WrapPromptChange(doc.getElementsByTagName("NEWSPLITSIGN").item(i).getFirstChild().getNodeValue());
      secttemp.m_strNewSplitSign = WrapPromptChange(secttemp.m_strNewSplitSign);

      if (doc.getElementsByTagName("FIELDS").item(i) != null)
      {
        Node fieldnode = doc.getElementsByTagName("FIELDS").item(i);
        this.m_SectTemplet.put(Integer.valueOf(secttemp.m_nSectTypeIndex), secttemp);
        ParseField(secttemp.m_FieldTemplet, fieldnode);
      }
      this.m_SectTemplet.put(Integer.valueOf(secttemp.m_nSectTypeIndex), secttemp);
    }
  }

  public void ParseField(Map<Integer, FieldTemplet> mapfield, Node CurrentNode)
  {
    NodeList Ssn = CurrentNode.getChildNodes();

    for (int nIndex = 0; nIndex < Ssn.getLength(); nIndex++)
    {
      Node tempnode = Ssn.item(nIndex);

      if ((tempnode.getNodeType() != 1) || 
        (!tempnode.getNodeName().toUpperCase().equals("FIELDITEM")))
        continue;
      FieldTemplet fieldtemp = new FieldTemplet();
      NodeList childnodeList = tempnode.getChildNodes();
      if (childnodeList != null)
      {
        for (int i = 0; i < childnodeList.getLength(); i++)
        {
          Node childnode = childnodeList.item(i);

          if (childnode.getNodeType() != 1) {
            continue;
          }
          String NodeName = childnode.getNodeName().toUpperCase();
          if (NodeName.equals("SUBFIELDS"))
          {
            if (existSubField(childnode)) {
              ParseField(fieldtemp.m_SubFieldTemplet, childnode);
            }
          }
          else
          {
            String strValue = getNodeValue(childnode);
            if (NodeName.equals("FIELDINDEX"))
            {
              if (strValue.trim().equals(""))
                fieldtemp.m_nFieldIndex = -1;
              else
                fieldtemp.m_nFieldIndex = Integer.parseInt(strValue);
            }
            else if (NodeName.equals("FIELDNAME"))
            {
              fieldtemp.m_strFieldName = strValue;
            }
            else if (NodeName.equals("PARSETYPE"))
            {
              if (strValue.trim().equals(""))
                fieldtemp.m_nParseType = -1;
              else
                fieldtemp.m_nParseType = Integer.parseInt(strValue);
            }
            else if (NodeName.equals("STARTPOS"))
            {
              if (strValue.trim().equals(""))
                fieldtemp.m_nStartPos = -1;
              else
                fieldtemp.m_nStartPos = Integer.parseInt(strValue);
            }
            else if (NodeName.equals("DATALENGTH"))
            {
              if (strValue.trim().equals(""))
                fieldtemp.m_nDataLength = -1;
              else
                fieldtemp.m_nDataLength = Integer.parseInt(strValue);
            }
            else if (NodeName.equals("HEADSIGN"))
            {
              fieldtemp.m_strHeadFieldSign = WrapPromptChange(strValue);
            }
            else if (NodeName.equals("TAILSIGN"))
            {
              fieldtemp.m_strTailFieldSign = WrapPromptChange(strValue);
            }
            else if (NodeName.equals("ROWSPLITSIGN"))
            {
              fieldtemp.m_strSubFieldRowSplitSign = WrapPromptChange(strValue);
            }
            else if (NodeName.equals("SUBISSPLIT"))
            {
              if (strValue.trim().equals("1"))
                fieldtemp.m_bSubSectSplit = true;
              else
                fieldtemp.m_bSubSectSplit = true;
            } else {
              if (!NodeName.equals("COLSPLITSIGN"))
                continue;
              fieldtemp.m_strSubFieldColSplitSign = WrapPromptChange(strValue);
            }
          }

        }

      }

      mapfield.put(Integer.valueOf(fieldtemp.m_nFieldIndex), fieldtemp);
    }
  }

  public class FieldTemplet
  {
    public int m_nFieldIndex;
    public String m_strFieldName;
    public int m_nParseType;
    public int m_nStartPos;
    public int m_nDataLength;
    public String m_strHeadFieldSign;
    public String m_strTailFieldSign;
    public int dataIndex;
    public String m_strSubFieldRowSplitSign;
    public boolean m_bSubSectSplit;
    public String m_strSubFieldColSplitSign;
    public Map<Integer, FieldTemplet> m_SubFieldTemplet = new HashMap();

    public FieldTemplet()
    {
    }
  }

  public class SectTemplet
  {
    public int m_nSectTypeIndex;
    public int m_nSectKeySearchType;
    public String headLine;
    public String m_strSectKeyWord;
    public String m_strNewSplitSign;
    public String m_strCommonFieldList;
    public Map<Integer, SectTempletP.FieldTemplet> m_FieldTemplet = new HashMap();

    public SectTemplet()
    {
    }
  }
}