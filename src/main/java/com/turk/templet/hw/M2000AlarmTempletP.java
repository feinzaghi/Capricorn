package com.turk.templet.hw;

import com.turk.Config.SystemConfig;

import java.io.File;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.templet.AbstractTempletBase;

public class M2000AlarmTempletP extends AbstractTempletBase
{
  public Map<Integer, TableTemplet> tableInfo = new HashMap();
  static Socket server;
  public static String ALARM_HANDSHAKE;
  public static String ALARM_NUM;
  public static String NETWORK_NUM;
  public static String OBJECT_IDCODE;
  public static String OBJECT_NAME;
  public static String OBJECT_TYPE;
  public static String NETWORK_IDCODE;
  public static String NETWORK_NAME;
  public static String NETWORK_TYPE;
  public static String ALARM_ID;
  public static String ALARM_CLASS;
  public static String ALARM_STATUS;
  public static String ALARM_TYPE_ID;
  public static String ALARM_TYPE;
  public static String BEGIN_TIME;
  public static String RESUME_DATE;
  public static String CONFIRM_DATE;
  public static String PITCH_INFO;
  public static String OPERATOR;
  public static String ALARM_NAME;
  public static String ALARM_CLASS_ID;
  public static String ALARM_LEVEL_ID;
  public static String ALARM_LEVEL;

  public void parseTemp(String tempFile)
    throws Exception
  {
    if ((tempFile == null) || (tempFile.trim().equals(""))) {
      return;
    }
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = null;

    String TempletFilePath = SystemConfig.getInstance().getTempletPath();
    TempletFilePath = TempletFilePath + File.separatorChar + tempFile;
    File file1 = new File(TempletFilePath);
    doc = builder.parse(file1);

    NodeList fieldFlagList = doc.getElementsByTagName("FIELDFLAG");
    if (fieldFlagList.getLength() >= 1)
    {
      if (doc.getElementsByTagName("ALARM_HANDSHAKE").item(0).getFirstChild() == null)
        ALARM_HANDSHAKE = "";
      else {
        ALARM_HANDSHAKE = doc.getElementsByTagName("ALARM_HANDSHAKE").item(0).getFirstChild().getNodeValue();
      }

      if (doc.getElementsByTagName("ALARM_NUM").item(0).getFirstChild() == null)
        ALARM_NUM = "";
      else {
        ALARM_NUM = doc.getElementsByTagName("ALARM_NUM").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("NETWORK_NUM").item(0).getFirstChild() == null)
        NETWORK_NUM = "";
      else {
        NETWORK_NUM = doc.getElementsByTagName("NETWORK_NUM").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("OBJECT_IDCODE").item(0).getFirstChild() == null)
        OBJECT_IDCODE = "";
      else {
        OBJECT_IDCODE = doc.getElementsByTagName("OBJECT_IDCODE").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("OBJECT_NAME").item(0).getFirstChild() == null)
        OBJECT_NAME = "";
      else {
        OBJECT_NAME = doc.getElementsByTagName("OBJECT_NAME").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("OBJECT_TYPE").item(0).getFirstChild() == null)
        OBJECT_TYPE = "";
      else {
        OBJECT_TYPE = doc.getElementsByTagName("OBJECT_TYPE").item(0).getFirstChild().getNodeValue();
      }

      if (doc.getElementsByTagName("NETWORK_IDCODE").item(0).getFirstChild() == null)
        NETWORK_IDCODE = "";
      else {
        NETWORK_IDCODE = doc.getElementsByTagName("NETWORK_IDCODE").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("NETWORK_NAME").item(0).getFirstChild() == null)
        NETWORK_NAME = "";
      else {
        NETWORK_NAME = doc.getElementsByTagName("NETWORK_NAME").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("NETWORK_TYPE").item(0).getFirstChild() == null)
        NETWORK_TYPE = "";
      else {
        NETWORK_TYPE = doc.getElementsByTagName("NETWORK_TYPE").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_ID").item(0).getFirstChild() == null)
        ALARM_ID = "";
      else {
        ALARM_ID = doc.getElementsByTagName("ALARM_ID").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_CLASS").item(0).getFirstChild() == null)
        ALARM_CLASS = "";
      else {
        ALARM_CLASS = doc.getElementsByTagName("ALARM_CLASS").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_STATUS").item(0).getFirstChild() == null)
        ALARM_STATUS = "";
      else {
        ALARM_STATUS = doc.getElementsByTagName("ALARM_STATUS").item(0).getFirstChild().getNodeValue();
      }

      if (doc.getElementsByTagName("ALARM_TYPE_ID").item(0).getFirstChild() == null)
        ALARM_TYPE_ID = "";
      else {
        ALARM_TYPE_ID = doc.getElementsByTagName("ALARM_TYPE_ID").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_TYPE").item(0).getFirstChild() == null)
        ALARM_TYPE = "";
      else {
        ALARM_TYPE = doc.getElementsByTagName("ALARM_TYPE").item(0).getFirstChild().getNodeValue();
      }

      if (doc.getElementsByTagName("ALARM_NAME").item(0).getFirstChild() == null)
        ALARM_NAME = "";
      else {
        ALARM_NAME = doc.getElementsByTagName("ALARM_NAME").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_CLASS_ID").item(0).getFirstChild() == null)
        ALARM_CLASS_ID = "";
      else {
        ALARM_CLASS_ID = doc.getElementsByTagName("ALARM_CLASS_ID").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_LEVEL_ID").item(0).getFirstChild() == null)
        ALARM_LEVEL_ID = "";
      else {
        ALARM_LEVEL_ID = doc.getElementsByTagName("ALARM_LEVEL_ID").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("ALARM_LEVEL").item(0).getFirstChild() == null)
        ALARM_LEVEL = "";
      else {
        ALARM_LEVEL = doc.getElementsByTagName("ALARM_LEVEL").item(0).getFirstChild().getNodeValue();
      }

      if (doc.getElementsByTagName("BEGIN_TIME").item(0).getFirstChild() == null)
        BEGIN_TIME = "";
      else {
        BEGIN_TIME = doc.getElementsByTagName("BEGIN_TIME").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("RESUME_DATE").item(0).getFirstChild() == null)
        RESUME_DATE = "";
      else {
        RESUME_DATE = doc.getElementsByTagName("RESUME_DATE").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("CONFIRM_DATE").item(0).getFirstChild() == null)
        CONFIRM_DATE = "";
      else {
        CONFIRM_DATE = doc.getElementsByTagName("CONFIRM_DATE").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("PITCH_INFO").item(0).getFirstChild() == null)
        PITCH_INFO = "";
      else {
        PITCH_INFO = doc.getElementsByTagName("PITCH_INFO").item(0).getFirstChild().getNodeValue();
      }
      if (doc.getElementsByTagName("OPERATOR").item(0).getFirstChild() == null)
        OPERATOR = "";
      else {
        OPERATOR = doc.getElementsByTagName("OPERATOR").item(0).getFirstChild().getNodeValue();
      }
    }
    NodeList tableNodeInfo = doc.getElementsByTagName("DATATABLE");

    for (int i = 0; i < tableNodeInfo.getLength(); i++)
    {
      TableTemplet table = new TableTemplet();

      if (doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild() == null)
        table.tableIndex = 0;
      else {
        table.tableIndex = Integer.parseInt(doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild().getNodeValue());
      }
      if (doc.getElementsByTagName("TABLENAME").item(i).getFirstChild() == null)
        table.strTableName = "";
      else {
        table.strTableName = doc.getElementsByTagName("TABLENAME").item(i).getFirstChild().getNodeValue();
      }
      if ((doc.getElementsByTagName("FILLTITLE") != null) && 
        (doc.getElementsByTagName("FILLTITLE").item(i) != null) && 
        (doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild() != null))
      {
        String strValue = doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild().getNodeValue();
        if (Integer.parseInt(strValue) == 1) {
          table.isFillTitle = true;
        }
      }
      if ((doc.getElementsByTagName("FIELDS").item(i) != null) && 
        (doc.getElementsByTagName("FIELDS").item(i).getFirstChild() != null))
      {
        Node fieldnode = doc.getElementsByTagName("FIELDS").item(i);
        parseFieldInfo(table.field, fieldnode);
      }
      this.tableInfo.put(Integer.valueOf(table.tableIndex), table);
    }
  }

  private void parseFieldInfo(Map<Integer, FieldTemplet> tableInfo, Node CurrentNode)
  {
    NodeList Ssn = CurrentNode.getChildNodes();

    for (int nIndex = 0; nIndex < Ssn.getLength(); nIndex++)
    {
      Node tempnode = Ssn.item(nIndex);

      if ((tempnode.getNodeType() != 1) || 
        (!tempnode.getNodeName().toUpperCase().equals("FIELDITEM")))
        continue;
      NodeList childnodeList = tempnode.getChildNodes();
      if (childnodeList == null)
        continue;
      FieldTemplet field = new FieldTemplet();
      for (int i = 0; i < childnodeList.getLength(); i++)
      {
        Node childnode = childnodeList.item(i);
        if (childnode.getNodeType() != 1)
          continue;
        String NodeName = childnode.getNodeName().toUpperCase();
        String strValue = getNodeValue(childnode);
        if (NodeName.equals("FIELDINDEX"))
        {
          if ((strValue == null) || (strValue.equals("")))
            field.fieldIndex = 0;
          else
            field.fieldIndex = Integer.parseInt(strValue);
        }
        else if (NodeName.equals("FIELDMAPPING"))
        {
          field.strFieldMapping = strValue;
        }
        else if (NodeName.equals("FIELDNAME"))
        {
          field.strFieldName = strValue;
        }
        else if (NodeName.equals("ISKEY"))
        {
          if (strValue.trim().equals("1"))
            field.isKey = true;
        }
        else if (NodeName.equals("KEYVALUE"))
        {
          field.strKeyValue = strValue;
        }
        else if (NodeName.equals("ISDEFAULT"))
        {
          if (strValue.trim().equals("1"))
            field.isDefault = true;
        }
        else if (NodeName.equals("DEFAULTVALUE"))
        {
          field.strDefaultValue = strValue;
        }
        else if (NodeName.equals("DATATYPE"))
        {
          if ((strValue == null) || (strValue.equals("")))
            field.dataType = 0;
          else
            field.dataType = Integer.parseInt(strValue);
        } else {
          if (!NodeName.equals("DATATIMEFORMAT"))
            continue;
          field.strDataTimeFormat = strValue;
        }
      }

      tableInfo.put(Integer.valueOf(field.fieldIndex), field);
    }
  }

  public class FieldTemplet
  {
    public int fieldIndex = 0;
    public String strFieldMapping = "";
    public String strFieldName = "";
    public boolean isKey = false;
    public String strKeyValue = "";
    public boolean isDefault = false;
    public String strDefaultValue = "";
    public int dataType = 0;
    public String strDataTimeFormat = "";

    public FieldTemplet()
    {
    }
  }

  public class TableTemplet
  {
    public int tableIndex;
    public String strTableName;
    public boolean isFillTitle = false;
    public Map<Integer, M2000AlarmTempletP.FieldTemplet> field = new HashMap();

    public TableTemplet()
    {
    }
  }
}