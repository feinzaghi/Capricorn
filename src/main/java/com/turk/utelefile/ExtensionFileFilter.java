package com.turk.utelefile;

import java.io.File;
import java.io.FileFilter;

public class ExtensionFileFilter implements FileFilter {


	private String extension;


	public ExtensionFileFilter(String extension) {
		this.extension = extension;
	}


	public boolean accept(File file) {
			if(file.isDirectory( )) {
					return false;
			}
			
			String name = file.getName( );
			
			return name.contains(this.extension);
			/*
			// find the last
			int index = name.lastIndexOf(".");
				if(index == -1) {
				return false;
				} else
				if(index == name.length( ) -1) {
				return false;
				} else {
					return this.extension.equals(name.substring(index+1));
				}*/
	}
} 