package com.turk.util;

import info.monitorenter.cpdetector.io.CodepageDetectorProxy; 
import info.monitorenter.cpdetector.io.JChardetFacade; 

import java.io.File;
import java.nio.charset.Charset; 

/**
 * 获取文件编码格式
 * @author Administrator
 *
 */
public class CharacterEnding {
	
	public static String getFileCharacterEnding(String filePath) 
	{
		File file = new File(filePath); 
		return getFileCharacterEnding(file);
	}
	/** 
	* Try to get file character ending.  
		Warning: use 
	* cpDetector to detect file's encoding. 
	* 
	* @param file 
	* @return 
	*/ 
	@SuppressWarnings("deprecation")
	public static String getFileCharacterEnding(File file) 
	{ 
		String fileCharacterEnding = "UTF-8"; 
		CodepageDetectorProxy detector = CodepageDetectorProxy.getInstance(); 
		detector.add(JChardetFacade.getInstance()); 
		Charset charset = null; 
		// File f = new File(filePath); 
		try 
		{ 
			charset = detector.detectCodepage(file.toURL()); 
		} 
		catch (Exception e) 
		{ 
			e.printStackTrace(); 
		} 
		if (charset != null) 
		{ 
			fileCharacterEnding = charset.name(); 
		} 
		return fileCharacterEnding; 
	} 

	public static void main(String[] args) { 
		String filePath = "D:\\Workspaces\\MyEclipse\\Capricorn\\data\\0_900001_20110630000000_1.txt"; 
		String type = CharacterEnding.getFileCharacterEnding(filePath); 
		System.out.println(type); 
		} 
}
