package com.turk.framework;


import com.turk.util.Util;
import com.turk.Config.SystemConfig;

/**
 * �汾����
 * @author Turk
 *
 */
public class Version
{
	//��ǰ�汾�ţ�ÿ�η����汾ʱ��Ҫ�޸Ĵ˴��汾��
	private static final String expectedVersion = "2.1.0.0";
	private static Version instance = null;

	public static synchronized Version getInstance()
	{
		if (instance == null)
		{
			instance = new Version();
		}

		return instance;
	}

	public boolean isRightVersion()
	{
		boolean bReturn = true;

		String version = SystemConfig.getInstance().getEdition();
		if (Util.isNotNull(version))
		{
			bReturn = expectedVersion.equals(version);
		}

		return bReturn;
	}

	public String getExpectedVersion()
	{
		return expectedVersion;
	}

	public static void main(String[] args)
	{
		System.out.println(getInstance().isRightVersion());
	}
}