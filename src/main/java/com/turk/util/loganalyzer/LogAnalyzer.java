package com.turk.util.loganalyzer;

import java.io.InputStream;

import com.turk.util.SqlldrResult;

/**
 * »’÷æ∑÷Œˆ
 * @author Administrator
 *
 */
public abstract interface LogAnalyzer
{
	public abstract void init()
    	throws LogAnalyzerException;

	public abstract SqlldrResult analysis(String paramString)
    	throws LogAnalyzerException;

	public abstract SqlldrResult analysis(InputStream paramInputStream)
    	throws LogAnalyzerException;

	public abstract void destory();
}