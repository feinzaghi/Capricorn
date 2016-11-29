package com.turk.utelefile;

import java.io.File;
import java.io.FileFilter;

public class ExtensionFolderFilter implements FileFilter {

	private String extension;


	public ExtensionFolderFilter(String extension) {
		this.extension = extension;
	}


	public boolean accept(File file) {
			if(file.isFile()) {
			     return false;
			}
			String name = file.getName();
			// find the last
			return name.contains(this.extension);
				
			}
}
