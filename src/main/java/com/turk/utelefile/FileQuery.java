package com.turk.utelefile;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;

public class FileQuery {
	
	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 查找指定目录下的文件夹
	 * @param currentPath
	 * @return
	 */
	public static File[] QueryFolder(String currentPath)
	{
		File[] files = null;
		File file = new File(currentPath);
		if(file.isDirectory())
		{
			files = file.listFiles();
		}
		return files;
	}
	
	/**
	 * 
	 * @param currentPath
	 * @return
	 */
	public static File[] QueryFolder(String currentPath,String filter)
	{
		File[] files = null;
		File file = new File(currentPath);
		if(file.isDirectory())
		{
			files = file.listFiles(new ExtensionFolderFilter(filter));
		}
		return files;
	}
	
	/**
	 * 
	 * @param currentPath
	 * @return
	 */
	public static File[] QueryFile(String currentPath,String filter)
	{
		File[] files = null;
		File file = new File(currentPath);
		
		if(file.isDirectory())
		{
			log.debug("QueryFile:"+currentPath + "   "+filter);
			files = file.listFiles(new ExtensionFileFilter(filter));
		}
		return files;
	}
}
