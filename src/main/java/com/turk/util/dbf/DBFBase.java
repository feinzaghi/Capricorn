package com.turk.util.dbf;

public abstract class DBFBase
{
  protected String characterSetName = "8859_1";
  protected final int END_OF_DATA = 26;

  public String getCharactersetName()
  {
    return this.characterSetName;
  }

  public void setCharactersetName(String characterSetName)
  {
    this.characterSetName = characterSetName;
  }
}