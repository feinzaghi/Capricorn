package com.turk.templet;

import java.util.Vector;

import com.turk.templet.LineTempletP.SubTemplet;

public abstract interface TempletBase
{
	public abstract void buildTmp(int paramInt);

	public abstract void buildTmp(TempletRecord paramTempletRecord);

	public abstract void parseTemp(String paramString)
    	throws Exception;
	

}