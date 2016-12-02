package com.turk.db.dao;

import java.util.ArrayList;
import java.util.List;

public class PageQueryResult<T>
{
  private int pageSize;
  private int currentPage;
  private int pageCount;
  private List<T> datas;

  public PageQueryResult()
  {
    this.datas = new ArrayList<T>();
  }

  public PageQueryResult(int pageSize, int currentPage, int pageCount, List<T> datas)
  {
    this.pageSize = pageSize;
    this.currentPage = currentPage;
    this.pageCount = pageCount;
    this.datas = datas;
  }

  public int getPageSize()
  {
    return this.pageSize;
  }

  public void setPageSize(int pageSize)
  {
    this.pageSize = pageSize;
  }

  public int getCurrentPage()
  {
    return this.currentPage;
  }

  public void setCurrentPage(int currentPage)
  {
    this.currentPage = currentPage;
  }

  public int getPageCount()
  {
    return this.pageCount;
  }

  public void setPageCount(int pageCount)
  {
    this.pageCount = pageCount;
  }

  public List<T> getDatas()
  {
    return this.datas;
  }

  public void setDatas(List<T> datas)
  {
    this.datas = datas;
  }
}