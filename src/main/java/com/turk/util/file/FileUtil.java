package com.turk.util.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileUtil
{
  public static boolean filerename(String sourfile, String newPath)
  {
    try
    {
      File oldFile = new File(sourfile);

      File fnewpath = new File(newPath);

      if (!fnewpath.exists()) {
        fnewpath.mkdirs();
      }
      File fnew = new File(newPath + File.separator + oldFile.getName());
      oldFile.renameTo(fnew);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public static void copyFileFromDir(String targetDir, String path)
  {
    File file = new File(path);
    createFile(targetDir, false);
    if (file.isDirectory())
      copyFileToDir(targetDir, listFile(file));
  }

  public static void copyFileOnly(String targetDir, String path)
  {
    File file = new File(path);
    File targetFile = new File(targetDir);
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      for (File subFile : files)
        if (subFile.isFile())
          copyFile(targetFile, subFile);
    }
  }

  public static void copyDir(String targetDir, String path)
  {
    File targetFile = new File(targetDir);
    createFile(targetFile, false);
    File file = new File(path);
    if ((targetFile.isDirectory()) && (file.isDirectory()))
      copyFileToDir(targetFile.getAbsolutePath() + "/" + file.getName(), 
        listFile(file));
  }

  public static void copyFileToDir(String targetDir, String[] filePath)
  {
    if ((targetDir == null) || ("".equals(targetDir)))
    {
      return;
    }
    File targetFile = new File(targetDir);
    if (!targetFile.exists()) {
      targetFile.mkdir();
    }
    else if (!targetFile.isDirectory())
    {
      return;
    }

    for (String path : filePath) {
      File file = new File(path);
      if (file.isDirectory())
        copyFileToDir(targetDir + "/" + file.getName(), listFile(file));
      else
        copyFileToDir(targetDir, file, "");
    }
  }

  public static void copyFileToDir(String targetDir, File file, String newName)
  {
    String newFile = "";
    if ((newName != null) && (!"".equals(newName)))
      newFile = targetDir + "/" + newName;
    else {
      newFile = targetDir + "/" + file.getName();
    }
    File tFile = new File(newFile);
    copyFile(tFile, file);
  }

  public static void copyFile(File targetFile, File file)
  {
    if (targetFile.exists())
    {
      return;
    }
    createFile(targetFile, true);
    try
    {
      InputStream is = new FileInputStream(file);
      FileOutputStream fos = new FileOutputStream(targetFile);
      byte[] buffer = new byte[1024];
      while (is.read(buffer) != -1) {
        fos.write(buffer);
      }
      is.close();
      fos.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String[] listFile(File dir) {
    String absolutPath = dir.getAbsolutePath();
    String[] paths = dir.list();
    String[] files = new String[paths.length];
    for (int i = 0; i < paths.length; i++) {
      files[i] = (absolutPath + "/" + paths[i]);
    }
    return files;
  }

  public static void createFile(String path, boolean isFile) {
    createFile(new File(path), isFile);
  }

  public static void createFile(File file, boolean isFile) {
    if (!file.exists())
      if (!file.getParentFile().exists()) {
        createFile(file.getParentFile(), false);
      }
      else if (isFile)
        try {
          file.createNewFile();
        } catch (IOException e) {
          e.printStackTrace();
        }
      else
        file.mkdir();
  }
}