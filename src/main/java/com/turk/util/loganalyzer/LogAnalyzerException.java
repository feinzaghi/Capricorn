package com.turk.util.loganalyzer;

public class LogAnalyzerException extends Exception
{
	private static final long serialVersionUID = -6141273162666604535L;

	public LogAnalyzerException()
	{
	}

	public LogAnalyzerException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public LogAnalyzerException(String message)
	{
		super(message);
	}

	public LogAnalyzerException(Throwable cause) {
		super(cause);
	}
}