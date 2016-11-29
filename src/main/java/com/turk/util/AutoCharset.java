package com.turk.util;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Hashtable;
import java.util.Set;

public class AutoCharset
{
	public static final String ORG_STRING = "is中文=1";
	private static Hashtable<String, CharsetMap> charsetHashtable = new Hashtable();

	static
	{
	    addCharsetHashtable(charsetHashtable, "ISO-8859-1", "UTF-8");
	    addCharsetHashtable(charsetHashtable, "ISO-8859-1", "UTF-16");
	    addCharsetHashtable(charsetHashtable, "ISO-8859-1", "GBK");
	
	    addCharsetHashtable(charsetHashtable, "UTF-8", "ISO-8859-1");
	    addCharsetHashtable(charsetHashtable, "UTF-8", "UTF-16");
	    addCharsetHashtable(charsetHashtable, "UTF-8", "GBK");
	
	    addCharsetHashtable(charsetHashtable, "UTF-16", "ISO-8859-1");
	    addCharsetHashtable(charsetHashtable, "UTF-16", "UTF-8");
	    addCharsetHashtable(charsetHashtable, "UTF-16", "GBK");
	
	    addCharsetHashtable(charsetHashtable, "GBK", "ISO-8859-1");
	    addCharsetHashtable(charsetHashtable, "GBK", "UTF-8");
	    addCharsetHashtable(charsetHashtable, "GBK", "UTF-16");
	}	

	private static void addCharsetHashtable(Hashtable<String, CharsetMap> hashtable, String orgCharset, String newCharset)
	{
		hashtable.put("is中文=1$" + orgCharset + "$" + newCharset, 
				new CharsetMapImpl(orgCharset, newCharset));
	}

	public static String getCorrectString(String orgString, String charsetCorrectString) 
	{
		String realString = null;

		if (!"is中文=1".equals(charsetCorrectString)) {
			String[] keys = (String[])charsetHashtable.keySet().toArray(new String[0]);
			for (int i = 0; i < keys.length; i++) {
				CharsetMap charsetMap = (CharsetMap)charsetHashtable.get(keys[i]);
				if ((charsetMap == null) || 
						(!"is中文=1".equals(charsetMap.map(charsetCorrectString)))) {
					continue;
				}
				realString = charsetMap.map(orgString);
				break;
			}

			if (realString == null) {
				System.out.println("not found charset");
				realString = orgString;
			}
		} else {
			realString = orgString;
		}

		return realString;
	}

	static abstract interface CharsetMap
	{
		public abstract String map(String paramString);
	}
	
	static class CharsetMapImpl
		implements AutoCharset.CharsetMap
    {
		private String orgCharset;
		private String newCharset;

		CharsetMapImpl(String orgCharset, String newCharset)
		{
			this.orgCharset = orgCharset;
			this.newCharset = newCharset;
		}

		public String map(String orgString) {
			try {
				return new String(orgString.getBytes(this.orgCharset), this.newCharset);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}return orgString;
		}
 	 }
}