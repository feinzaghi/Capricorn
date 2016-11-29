package com.turk.util.string;

import java.util.Enumeration;
import java.util.StringTokenizer;

public class PowerfulTokenizer
  	implements Enumeration<String>
{
	private String sInput;
	private String sDelim;
	private boolean bIncludeDelim = false;
	private StringTokenizer oTokenizer;
	private int iEndQuote = 0;
	private String sPrevToken = "";
	private int iTokenNo = 0;
	private int iTotalTokens = 0;
	private int iTokens = 0;
	private int iLen = 0;

	public PowerfulTokenizer(String str, String sep)
	{
		this.sInput = str;
		this.sDelim = sep;
		this.iLen = this.sDelim.length();
		this.oTokenizer = new StringTokenizer(str, sep, true);
	}

	public PowerfulTokenizer(String str, String sep, boolean bIncludeDelim)
	{
		this.sInput = str;
		this.sDelim = sep;
		this.bIncludeDelim = bIncludeDelim;
		this.iLen = this.sDelim.length();
		this.oTokenizer = new StringTokenizer(str, sep, true);
	}

	public String nextToken()
	{
		String sToken = this.oTokenizer.nextToken();

		if ((this.sPrevToken.equals(this.sDelim)) && (sToken.equals(this.sDelim)))
		{
			this.sPrevToken = sToken;
			this.iTokenNo += 1;
			return "";
		}

		if ((sToken.trim().startsWith("\"")) && (sToken.length() == 1))
		{
			String sNextToken = this.oTokenizer.nextToken();
			while (!sNextToken.trim().endsWith("\""))
			{
				sToken = sToken + sNextToken;
				sNextToken = this.oTokenizer.nextToken();
			}
			sToken = sToken + sNextToken;
			this.sPrevToken = sToken;
			this.iTokenNo += 1;
			return sToken.substring(1, sToken.length() - 1);
		}

		if ((sToken.trim().startsWith("\"")) && (
				(!sToken.trim().endsWith("\"")) || (sToken.trim().endsWith("\"\""))))
		{
			if (this.oTokenizer.hasMoreTokens())
			{
				String sNextToken = this.oTokenizer.nextToken();

				while ((!sNextToken.trim().endsWith("\"")) || (sNextToken.trim().endsWith("\"\"")))
				{
					sToken = sToken + sNextToken;
					if (!this.oTokenizer.hasMoreTokens())
					{
						sNextToken = "";
						break;
					}
					sNextToken = this.oTokenizer.nextToken();
				}
				sToken = sToken + sNextToken;
			}
		}

		this.sPrevToken = sToken;

		if (sToken.length() > 0)
		{
			sToken = sToken.trim();

			if ((sToken.charAt(0) == '"') && 
					(sToken.charAt(sToken.length() - 1) == '"')) {
				sToken = sToken.substring(1, sToken.length() - 1);
			}
			String sTemp = "";
			int iPrevDblQuote = 0;
			int iDblQuote = sToken.indexOf("\"\"");

			if (iDblQuote != -1)
			{
				String sDummy = sToken;
				while (iDblQuote != -1)
				{
					sTemp = sDummy.substring(0, iDblQuote + 1);
					sTemp = sTemp + sDummy.substring(iDblQuote + 2);
					iPrevDblQuote = iDblQuote;
					sDummy = sTemp;
					iDblQuote = sDummy.indexOf("\"\"", iPrevDblQuote + 1);
				}
				sToken = sTemp;
			}
		}

		if ((!this.bIncludeDelim) && (sToken.equals(this.sDelim)))
		{
			sToken = nextToken();
		}
		else {
			this.iTokenNo += 1;
		}
		return sToken;
	}

	public boolean hasMoreTokens()
	{
		if (this.iTotalTokens == 0) {
			this.iTotalTokens = countTokens();
		}
		return this.iTokenNo < this.iTotalTokens;
	}

	public boolean hasMoreElements()
	{
		return hasMoreTokens();
	}	

	public String nextElement()
	{
		return nextToken();
	}

	public int countTokens()
	{
		this.iTokens = this.oTokenizer.countTokens();
		int iActualTokens = this.iTokens;

		int[] aiIndex = new int[this.iTokens];
		aiIndex[0] = 0;
		int iIndex = 0;
		int iNextIndex = 0;

		for (int i = 1; i < aiIndex.length; i++)
		{
			iIndex = this.sInput.indexOf(this.sDelim, iIndex + 1);
			if (iIndex == -1) {
				break;
			}
			do
			{
				iNextIndex = this.sInput.indexOf(this.sDelim, iIndex + 1);
				if (iNextIndex == -1)
					break;
				iIndex = iNextIndex;
			}
			while (this.sInput.substring(iIndex - this.iLen, iIndex).equals(this.sDelim));

			aiIndex[i] = iIndex;

			if (!isWithinQuotes(iIndex))
				continue;
			if (this.bIncludeDelim)
				this.iTokens -= 2;
			else {
				this.iTokens -= 1;
			}
		}

		if (this.bIncludeDelim)
		{
			return this.iTokens;
		}
		if ((!this.bIncludeDelim) || (this.iTokens == iActualTokens))
		{
			int iIdx = 0;
			iIdx = this.sInput.indexOf(this.sDelim, iIdx + 1);
			while (iIdx != -1)
			{
				if ((this.sInput.charAt(iIdx - 1) != '"') || 
						(this.sInput.charAt(iIdx + 1) != '"') || 
						(iIdx + 1 + this.iLen > this.sInput.length()) || 
								(!this.sInput.substring(iIdx + 1, iIdx + 
										1 + this.iLen).equals(this.sDelim)))
				{
					this.iTokens -= 1;
				}

				while ((iIdx + 1 < this.sInput.length()) && (
						this.sInput.substring(iIdx + 1, iIdx + 1 + this.iLen).equals(this.sDelim)))
				{
					iIdx += this.iLen;
				}
				iIdx = this.sInput.indexOf(this.sDelim, iIdx + 1);
			}		
		}

		return this.iTokens;
	}

	private boolean isWithinQuotes(int k)
	{
		int iStartQuote = this.sInput.indexOf("\"", 0);

		if (k < iStartQuote) {
			return false;
		}
		if (!this.bIncludeDelim)
		{
			if ((this.sInput.charAt(k - 1) == '"') && 
					(this.sInput.charAt(k + 1) == '"') && 
					(k + 1 + this.iLen <= this.sInput.length()) && 
					(this.sInput.substring(k + 1, k + 1 + this.iLen).equals(this.sDelim)))
			{
				this.iTokens -= 2;
				return false;
			}
		}

		do
		{
			this.iEndQuote = this.sInput.indexOf("\"", iStartQuote + 1);

			if ((k > iStartQuote) && (k < this.iEndQuote))
			{
				return true;
			}

			iStartQuote = this.sInput.indexOf("\"", this.iEndQuote + 1);
		}
		while (iStartQuote != -1);

		return false;
	}
}