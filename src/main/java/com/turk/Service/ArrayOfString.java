package com.turk.Service;

public class ArrayOfString {
	 	protected String[] string;

	    public String[] getString() {
	        if (string == null) {
	            string = new String[0];
	        }
	        return this.string;
	    }


	    public void setString(String[] string) {
	        this.string = string;
	    }
}
