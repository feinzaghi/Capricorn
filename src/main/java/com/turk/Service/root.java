package com.turk.Service;

import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class root {
	public String version = "";
	public String status = "";
	public String sessionId = "";
	public String successTotal = "";
	public String failureTotal = "";
	public String successList = "";
	public String failureList = "";
}
