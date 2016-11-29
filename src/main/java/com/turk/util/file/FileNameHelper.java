package com.turk.util.file;

import java.io.File;

public class FileNameHelper
{
  public static String getExt(String fileName)
  {
    fileName = new File(fileName).getName();
    String ext = "";
    if (fileName.lastIndexOf(".") > -1)
    {
      ext = fileName.substring(fileName.lastIndexOf(".") + 1);
    }
    return ext;
  }
}