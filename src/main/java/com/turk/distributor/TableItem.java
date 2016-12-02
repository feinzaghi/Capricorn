package com.turk.distributor;

import java.io.FileOutputStream;
import java.util.List;

public class TableItem
{
	public int tableIndex = 0;
	public FileOutputStream fileWriter = null;
	public String fileName = "";
	public String sql = "";
	public String outputFileName = "";
	public int recordCounts = 0;
	public List<String> head;
}