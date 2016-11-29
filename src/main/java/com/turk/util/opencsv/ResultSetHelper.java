package com.turk.util.opencsv;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract interface ResultSetHelper
{
	public abstract String[] getColumnNames(ResultSet paramResultSet)
    	throws SQLException;

	public abstract String[] getColumnValues(ResultSet paramResultSet)
    	throws SQLException, IOException;
}