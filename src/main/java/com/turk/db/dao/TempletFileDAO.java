package com.turk.db.dao;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.turk.config.SystemConfig;
import com.turk.db.pojo.TempletFile;
import com.turk.util.Util;

public class TempletFileDAO extends AbstractDAO<TempletFile>
{
  private String TempletFolderPath = SystemConfig.getInstance().getTempletPath();
  public static final long MAX_ALLOW_FILE_SIZE = 500000L;

  public List<TempletFile> criteriaQuery(TempletFile tmpFile)
  {
    String name = null;
    boolean queryFlag = false;
    if (tmpFile != null)
    {
      name = tmpFile.getName();
      if (Util.isNotNull(name)) {
        queryFlag = true;
      }
    }
    File folder = new File(this.TempletFolderPath);
    if ((!folder.exists()) || (folder.isFile())) {
      return null;
    }
    List<TempletFile> tFiles = new ArrayList<TempletFile>();

    File[] files = folder.listFiles();
    for (File f : files)
    {
      if (f.isDirectory()) {
        continue;
      }
      TempletFile e = new TempletFile();
      if (queryFlag)
      {
        String fName = f.getName();

        if (fName.indexOf(name) <= -1)
          continue;
        e.setName(fName);
        e.setSize(f.length());
        e.setModifyDate(Util.getDateString(new Date(f.lastModified())));
        tFiles.add(e);
      }
      else
      {
        e.setName(f.getName());
        e.setSize(f.length());
        e.setModifyDate(Util.getDateString(new Date(f.lastModified())));
        tFiles.add(e);
      }
    }

    return tFiles;
  }

  public boolean delete(TempletFile entity)
  {
    if ((entity == null) || (Util.isNull(entity.getName())))
    {
      return false;
    }

    String name = entity.getName();
    File f = new File(this.TempletFolderPath + File.separator + name);
    if ((f.exists()) && (f.isFile()))
    {
      return f.delete();
    }

    return false;
  }

  public boolean exists(TempletFile entity)
  {
    if ((entity == null) || (Util.isNull(entity.getName()))) {
      return false;
    }
    String name = entity.getName();
    File f = new File(this.TempletFolderPath + File.separator + name);

    return f.exists();
  }

  public TempletFile getByName(String name)
  {
    if (Util.isNull(name)) {
      return null;
    }
    String fPath = this.TempletFolderPath + File.separator + name;
    File f = new File(fPath);
    if ((f.exists()) && (f.isFile()))
    {
      TempletFile e = new TempletFile();
      e.setName(f.getName());
      e.setSize(f.length());
      e.setModifyDate(Util.getDateString(new Date(f.lastModified())));
      if (f.length() <= 500000L)
        e.setContent(getContent(fPath));
      else {
        e.setContent("文件内容超过指定最大值(500000 Byte),建议把文件下载本地进行编辑,之后再上传到采集机上!");
      }

      return e;
    }

    return null;
  }

  public boolean update(TempletFile entity)
  {
    String fileName = entity.getName();
    String content = entity.getContent();

    if (Util.isNull(fileName)) {
      return false;
    }
    if (content == null) {
      content = "";
    }
    boolean ret = true;
    String fPath = this.TempletFolderPath + File.separator + fileName;
    OutputStream fout = null;
    try
    {
      fout = new FileOutputStream(fPath);
      byte[] b = content.getBytes();
      fout.write(b, 0, b.length);
      fout.flush();
    }
    catch (FileNotFoundException e)
    {
      ret = false;

      if (fout != null)
      {
        try
        {
          fout.close();
        }
        catch (IOException localIOException1)
        {
        }
      }
    }
    catch (IOException e)
    {
      ret = false;

      if (fout != null)
      {
        try
        {
          fout.close();
        }
        catch (IOException localIOException2)
        {
        }
      }
    }
    finally
    {
      if (fout != null)
      {
        try
        {
          fout.close();
        }
        catch (IOException localIOException3)
        {
        }
      }
    }

    return ret;
  }

  private String getContent(String fileName)
  {
    if (Util.isNull(fileName)) {
      return null;
    }
    StringBuffer content = new StringBuffer();
    BufferedReader br = null;
    try
    {
      String strLine = null;
      br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileName))));

      while ((strLine = br.readLine()) != null)
      {
        content.append(strLine).append(System.getProperty("line.separator"));
      }
    }
    catch (FileNotFoundException e)
    {
      content.append("文件不存在,详细:").append(e.getMessage());

      if (br != null)
      {
        try
        {
          br.close();
        }
        catch (IOException localIOException1)
        {
        }
      }
    }
    catch (IOException e)
    {
      content.append("文件访问出错,详细:").append(e.getMessage());

      if (br != null)
      {
        try
        {
          br.close();
        }
        catch (IOException localIOException2)
        {
        }
      }
    }
    finally
    {
      if (br != null)
      {
        try
        {
          br.close();
        }
        catch (IOException localIOException3)
        {
        }
      }
    }

    return content.toString();
  }

  public boolean isExceedMaxValue(String fileName)
  {
    if (Util.isNull(fileName)) {
      return false;
    }
    String fPath = this.TempletFolderPath + File.separator + fileName;
    File f = new File(fPath);
    if ((f.exists()) && (f.isFile()))
    {
      return f.length() > 500000L;
    }

    return false;
  }

  public boolean rename(String oldFileName, String newFileName)
  {
    if ((Util.isNull(oldFileName)) || (Util.isNull(newFileName))) {
      return false;
    }
    String fOldPath = this.TempletFolderPath + File.separator + oldFileName;
    File fOld = new File(fOldPath);
    if ((!fOld.exists()) || (!fOld.isFile())) {
      return false;
    }
    String fNewPath = this.TempletFolderPath + File.separator + newFileName;
    File fNew = new File(fNewPath);

    if ((fNew.exists()) && (fNew.isFile())) {
      return false;
    }
    return fOld.renameTo(fNew);
  }

  public boolean newFile(String newFileName)
  {
    if (Util.isNull(newFileName)) {
      return false;
    }
    String fNewPath = this.TempletFolderPath + File.separator + newFileName;
    File fNew = new File(fNewPath);

    if ((fNew.exists()) && (fNew.isFile())) {
      return false;
    }
    boolean b = true;
    try
    {
      b = fNew.createNewFile();
    }
    catch (IOException e)
    {
      b = false;
    }

    return b;
  }

  public String getTempletFolderPath()
  {
    return this.TempletFolderPath;
  }
}