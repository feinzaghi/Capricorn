package com.turk.templet;

import java.io.Serializable;

public class TempletRecord
implements Serializable
{
 
	/**
	 * 
	 */
	private static final long serialVersionUID = -2674889367035720962L;
	private int id;
	private int type;
	private String name;
	private String edition;
	private String fileName;

  public int getId()
  {
    return this.id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public int getType()
  {
    return this.type;
  }

  public void setType(int type)
  {
    this.type = type;
  }

  public String getName()
  {
    return this.name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getEdition()
  {
    return this.edition;
  }

  public void setEdition(String edition)
  {
    this.edition = edition;
  }

  public String getFileName()
  {
    return this.fileName;
  }

  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }
}