package com.turk.parser;

import com.turk.config.ConstDef;
import com.turk.distributor.GenericSectionHeadDistributor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import com.turk.exception.ParseException;
import com.turk.task.CollectObjInfo;
import com.turk.task.DevInfo;
import com.turk.templet.AbstractTempletBase;
import com.turk.templet.GenericSectionHeadD;
import com.turk.templet.GenericSectionHeadP;
import com.turk.templet.GenericSectionHeadP.Templet;
import com.turk.templet.Table;
import com.turk.templet.TempletBase;
import com.turk.util.CSVLineParser;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class GenericSectionHeadParser extends Parser
{
  private StringBuilder strBuffer = new StringBuilder();

  private boolean isPublicStart = false;
  private boolean isPublicEnd = false;
  private boolean isPublicFinished = false;

  private boolean isDataAreaStartFound = false;
  private boolean isDataAreaEndFound = false;

  private int currDsID = -1;

  private boolean lastCommit = false;
  private GenericSectionHeadP.Templet lastATempletP;
  private List<Integer> dsHandled = new ArrayList<Integer>();

  public GenericSectionHeadParser()
  {
  }

  public GenericSectionHeadParser(CollectObjInfo obj)
  {
    this.collectObjInfo = obj;
    this.distribute = new GenericSectionHeadDistributor(obj);
  }

  

  private List<GenericSectionHeadP.Templet> getTempletByFileName(GenericSectionHeadP templetP, String localFilename)
  {
    List<Templet> aTempletPs = new ArrayList<Templet>();

    Map<String, ?> templets = templetP.getTemplets();
    Set<String> fileNames = templets.keySet();
    for (String fName : fileNames)
    {
      if ((!getDsConfigName().equalsIgnoreCase(fName.trim())) && 
        (!logicEquals(getDsConfigName(), ConstDef.ParseFilePath(fName.trim(), this.collectObjInfo.getLastCollectTime()))) && 
        (!ConstDef.ParseFilePath(fName.trim(), this.collectObjInfo.getLastCollectTime()).endsWith(FilenameUtils.getName(localFilename))))
        continue;
      aTempletPs.add((GenericSectionHeadP.Templet)templets.get(fName));
    }

    return aTempletPs;
  }

  public boolean parseData()
  {
    if (Util.isNull(this.fileName)) {
      return false;
    }
    int taskID = this.collectObjInfo.getTaskID();

    TempletBase tBaseP = this.collectObjInfo.getParseTemplet();
    if (!(tBaseP instanceof GenericSectionHeadP))
      return false;
    GenericSectionHeadP templetP = (GenericSectionHeadP)tBaseP;

    List<GenericSectionHeadP.Templet> aTempletPs = getTempletByFileName(templetP, this.fileName);
    if (aTempletPs.size() == 0)
    {
      this.log.error(taskID + ": 解析失败,原因：找不到对应的解析模板." + this.fileName);
      return false;
    }

    boolean flag = true;
    for (GenericSectionHeadP.Templet aTempletP : aTempletPs)
    {
      TempletBase tBaseD = this.collectObjInfo.getDistributeTemplet();
      if (!(tBaseD instanceof GenericSectionHeadD))
      {
        flag = false;
      }
      else
      {
        GenericSectionHeadD templetD = (GenericSectionHeadD)tBaseD;

        int pTid = aTempletP.getId();

        GenericSectionHeadD.Templet aTempletD = templetD.getTemplet(pTid);
        if (aTempletD == null)
        {
          this.log.error(taskID + ": 解析失败:原因:找不到对应的分发模板. 解析模板编号=" + pTid);
          flag = false;
        }
        else
        {
          InputStream inn = null;
          try
          {
            inn = new FileInputStream(this.fileName);
            List lis = IOUtils.readLines(inn);
            if ((lis == null) || (lis.size() <= 1)) {
              LogMgr.getInstance().getDBLogger().log(this.collectObjInfo.getDevInfo().getDevID(), 
            		  ((Table)aTempletD.getTables().get(Integer.valueOf(0))).getName(), 
            		  this.collectObjInfo.getLastCollectTime(), 0, this.collectObjInfo.getTaskID(),0);
            }
            else {
              int cc = 0;
              for (Iterator localIterator2 = lis.iterator(); localIterator2.hasNext(); ) { Object o = localIterator2.next();

                if (o == null)
                  continue;
                String ss = o.toString();
                if (Util.isNotNull(ss)) {
                  cc++;
                }
              }
              if (cc <= 1)
                LogMgr.getInstance().getDBLogger().log(this.collectObjInfo.getDevInfo().getDevID(), 
                		((Table)aTempletD.getTables().get(Integer.valueOf(0))).getName(), 
                		this.collectObjInfo.getLastCollectTime(), 0, this.collectObjInfo.getTaskID(),0);
            }
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
          finally
          {
            IOUtils.closeQuietly(inn);
          }

          this.strBuffer.delete(0, this.strBuffer.length());
          BufferedReader br = null;
          try
          {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName)));
            String strLine = null;

            GenericSectionHeadP.Public publicEle = aTempletP.getPublicElement();

            char[] buf = new char[1024];
            int readCount = -1;
            while ((readCount = br.read(buf)) != -1)
            {
              if (aTempletP.getDsMap().size() == this.dsHandled.size()) {
                break;
              }
              this.strBuffer.append(buf, 0, readCount);

              while (!this.isPublicFinished)
              {
                handlePublicArea(publicEle);
                if ((!this.isPublicEnd) && (!this.isPublicFinished))
                  break;
              }
              if (!this.isPublicEnd) {
                continue;
              }

              do
              {
                if (strLine.trim().length() != 0)
                  continue;
              }
              while ((strLine = handleLine(aTempletP)) != null);
            }

            if (this.lastCommit)
            {
              handleALineData(this.lastATempletP, this.strBuffer.toString());
              this.distribute.commit();
            }

          }
          catch (FileNotFoundException e)
          {
            this.log.error(taskID + ": 解析失败,原因:文件不存在:" + this.fileName);

            this.strBuffer.delete(0, this.strBuffer.length());
            this.dsHandled.clear();
            resetState();
            if (br == null)
              continue;
            try
            {
              br.close();
            }
            catch (IOException localIOException1)
            {
            }
          }
          catch (IOException e)
          {
            this.log.error(taskID + ": 解析失败,原因:", e);

            this.strBuffer.delete(0, this.strBuffer.length());
            this.dsHandled.clear();
            resetState();
            if (br == null)
              continue;
            try
            {
              br.close();
            }
            catch (IOException localIOException2)
            {
            }
          }
          catch (Exception e)
          {
            this.log.error(taskID + ": 解析失败,原因：", e);

            this.strBuffer.delete(0, this.strBuffer.length());
            this.dsHandled.clear();
            resetState();
            if (br == null)
              continue;
            try
            {
              br.close();
            }
            catch (IOException localIOException3)
            {
            }
          }
          finally
          {
            this.strBuffer.delete(0, this.strBuffer.length());
            this.dsHandled.clear();
            resetState();
            if (br != null)
            {
              try
              {
                br.close();
              }
              catch (IOException localIOException4) {
              }
            }
          }
        }
      }
    }
    return flag;
  }

  private void resetState()
  {
    this.log.debug(this.collectObjInfo.getTaskID() + ": isDataAreaStartFound=" + 
      this.isDataAreaStartFound + " isDataAreaEndFound=" + 
      this.isDataAreaEndFound + " currDsID=" + this.currDsID);

    this.isPublicStart = false;
    this.isPublicEnd = false;
    this.isPublicFinished = false;
    this.currDsID = -1;
    this.lastCommit = false;
    this.lastATempletP = null;

    this.isDataAreaStartFound = false;
    this.isDataAreaEndFound = false;
  }

  private void handlePublicArea(GenericSectionHeadP.Public publicEle)
  {
    if (publicEle == null)
    {
      this.isPublicFinished = true;
      this.isPublicStart = true;
      this.isPublicEnd = true;
      return;
    }

    if (!this.isPublicFinished)
    {
      String startSign = publicEle.getStartSign();
      String endSign = publicEle.getEndSign();

      if ((!this.isPublicStart) || (!this.isPublicEnd))
      {
        if (this.isPublicStart)
        {
          this.isPublicEnd = (this.strBuffer.indexOf(endSign) > -1);
        }
        else
        {
          this.isPublicStart = (this.strBuffer.indexOf(startSign) > -1);
          if (this.isPublicStart) {
            this.isPublicEnd = (this.strBuffer.indexOf(endSign) > -1);
          }
        }
      }
      else
      {
        handlePublicData(publicEle);

        this.strBuffer.delete(0, this.strBuffer.indexOf(endSign) + 
          endSign.length());
        this.isPublicFinished = true;
      }
    }
  }

  private String handleLine(GenericSectionHeadP.Templet aTempletP)
    throws Exception
  {
    String strLine = null;

    if (!this.isDataAreaStartFound)
    {
      strLine = readLine(this.strBuffer);

      if (strLine == null) {
        return null;
      }

      this.currDsID = getDsFromLineData(aTempletP, strLine);

      if (this.currDsID > -1)
      {
        if (this.dsHandled.contains(Integer.valueOf(this.currDsID))) {
          throw new ParseException("模板配置有误." + 
            strLine);
        }
        this.isDataAreaStartFound = true;

        findFieldIndexInHead(aTempletP, strLine);
      }

    }
    else if (!this.isDataAreaEndFound)
    {
      GenericSectionHeadP.DS ds = (GenericSectionHeadP.DS)aTempletP.getDsMap().get(Integer.valueOf(this.currDsID));
      String endSign = ds.getMeta().getEndSign();
      int ePos = -1;
      if (endSign == null)
      {
        ePos = -1;
        this.lastCommit = true;
        this.lastATempletP = aTempletP;
      }
      else {
        ePos = this.strBuffer.indexOf(endSign);
      }this.isDataAreaEndFound = (ePos > -1);

      if (this.isDataAreaEndFound)
      {
        StringBuilder tmpBuffer = new StringBuilder();

        tmpBuffer.append(this.strBuffer.substring(0, ePos)).append("\n");
        this.strBuffer.delete(0, ePos + endSign.length());
        String tmpLine = null;
        while ((tmpLine = readLine(tmpBuffer)) != null)
        {
          handleALineData(aTempletP, tmpLine);
        }

        if (this.strBuffer.length() > 0)
          strLine = "";
        else {
          strLine = null;
        }
        this.dsHandled.add(Integer.valueOf(this.currDsID));
        this.currDsID = -1;
        this.isDataAreaStartFound = false;
        this.isDataAreaEndFound = false;
        this.distribute.commit();
      }
      else
      {
        strLine = readLine(this.strBuffer);

        if (strLine == null) {
          return null;
        }

        handleALineData(aTempletP, strLine);
      }

    }

    return strLine;
  }

  private String readLine(StringBuilder buffer)
  {
    int endPos = buffer.indexOf("\n");
    if (endPos == -1) {
      return null;
    }
    String strLine = buffer.substring(0, endPos);
    buffer.delete(0, endPos + 1);

    strLine = strLine.replace("\n", " ");
    strLine = strLine.replace("\r", " ");
    return strLine;
  }

  private void handleALineData(GenericSectionHeadP.Templet aTempletP, String strLine)
  {
    if (Util.isNull(strLine)) {
      return;
    }
    String splitSign = ((GenericSectionHeadP.DS)aTempletP.getDsMap().get(Integer.valueOf(this.currDsID))).getFields().getSplitSign();

    String[] strFieldValues = CSVLineParser.splitCSV(strLine, splitSign.toCharArray()[0]);
    int len = strFieldValues.length;
    Collection<GenericSectionHeadP.Field> cFields = ((GenericSectionHeadP.DS)aTempletP.getDsMap().get(Integer.valueOf(this.currDsID))).getFields().getFields().values();
    for (GenericSectionHeadP.Field f : cFields)
    {
      int indexInHead = f.getIndexInHead();
      if ((indexInHead <= -1) || (indexInHead >= len))
        continue;
      f.setValue(strFieldValues[indexInHead].trim());
    }

    try
    {
      this.distribute.distribute(aTempletP, Integer.valueOf(this.currDsID));
    }
    catch (Exception e)
    {
      this.log.error(this.collectObjInfo.getTaskID() + ": 分发数据异常,原因:", e);
    }
  }

  private int getDsFromLineData(GenericSectionHeadP.Templet aTempletP, String strLine)
  {
    String line = strLine.toLowerCase();
    int id = -1;
    Collection<GenericSectionHeadP.DS> cDS = aTempletP.getDsMap().values();
    for (GenericSectionHeadP.DS ds : cDS)
    {
      List<String> roList = ds.getRequiredOccur();
      boolean found = true;
      for (String ro : roList)
      {
        if (line.indexOf(ro.toLowerCase()) != -1)
          continue;
        found = false;
        break;
      }

      if (!found)
        continue;
      id = ds.getId();
      break;
    }

    return id;
  }

  private void findFieldIndexInHead(GenericSectionHeadP.Templet aTempletP, String strLine)
  {
    String splitSign = ((GenericSectionHeadP.DS)aTempletP.getDsMap().get(Integer.valueOf(this.currDsID))).getMeta().getHeadSplitSign();
    String[] strFieldNames = strLine.replace("\"", "").split(splitSign);
    Collection<GenericSectionHeadP.Field> cFields = ((GenericSectionHeadP.DS)aTempletP.getDsMap().get(Integer.valueOf(this.currDsID))).getFields().getFields().values();
    int len = strFieldNames.length;

    for (GenericSectionHeadP.Field f : cFields)
    {
      String fName = f.getName();
      for (int i = 0; i < len; i++)
      {
        String strFName = strFieldNames[i].trim();
        if (!strFName.equalsIgnoreCase(fName))
          continue;
        f.setIndexInHead(i);
        break;
      }
    }
  }

  private void handlePublicData(GenericSectionHeadP.Public publicEle)
  {
    GenericSectionHeadP.Fields fields = publicEle.getFields();
    if ((fields == null) || (fields.getFields() == null) || 
      (fields.getFields().size() <= 0)) {
      return;
    }
    Set<Map.Entry<Integer,GenericSectionHeadP.Field>> fieldSet 
    	= fields.getFields().entrySet();
    for (Map.Entry entry : fieldSet)
    {
      GenericSectionHeadP.Field field = (GenericSectionHeadP.Field)entry.getValue();
      String startSign = field.getStartSign();
      String endSign = field.getEndSign();
      int beginIndex = this.strBuffer.indexOf(startSign);
      int endIndex = this.strBuffer.indexOf(endSign, beginIndex);
      String fValue = this.strBuffer.substring(beginIndex + startSign.length(), endIndex);
      fValue = fValue.trim();
      field.setValue(fValue);
    }
  }

  public static void main(String[] args)
  {
    CollectObjInfo obj = new CollectObjInfo(999);
    obj.setLastCollectTime(new Timestamp(new Date().getTime()));
    obj.setDevInfo(new DevInfo());

    AbstractTempletBase pTemp = new GenericSectionHeadP();
    pTemp.buildTmp(407003);
    obj.setParseTemplet(pTemp);

    GenericSectionHeadD dTemp = new GenericSectionHeadD();
    dTemp.buildTmp(407004);
    obj.setDistributeTemplet(dTemp);

    GenericSectionHeadParser parser = new GenericSectionHeadParser(obj);
    parser.fileName = "C:\\Users\\Desktop\\Domain124_Handoverstatisticmeasurement_15Aug2010_1800-15Aug2010_1900.csv";

    parser.setDsConfigName("/export/home/omc/tmp/ftp/csv/ftp/Domain*_Handoverstatisticmeasurement_%%D%%EM%%Y_%%H%%m-%%ND%%NEM%%NY_%%NH%%Nm.csv|3600000");

    parser.parseData();
    System.out.println(10);
    System.out.println(13);
  }

@Override
public void Stop() {
	// TODO Auto-generated method stub
	
}
}