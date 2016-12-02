package com.turk.db.dao;

import java.util.List;

public class RTaskPageQueryResult<T> extends PageQueryResult
{
  private int recordCount;

  @SuppressWarnings("unchecked")
  public RTaskPageQueryResult(int pageSize, int currentPage, int pageCount, List<T> datas)
  {
	  super(pageSize, currentPage, pageCount, datas);
  }

  public RTaskPageQueryResult(int pageSize, int currentPage, int pageCount, int recordCount, List<T> datas)
  {
    this(pageSize, currentPage, pageCount, datas);
    this.recordCount = recordCount;
  }

  public String getPageInfo()
  {
    StringBuilder sb = new StringBuilder("<span style='float: left'>��");
    sb.append(this.recordCount + "��</span>");
    sb.append("<span style='float: right'> <a href='#' id='firstPage'>|&lt;&lt;��ҳ</a>&nbsp;&nbsp;&nbsp;");
    sb.append("<a href='#' id='frontPage'>&lt;&lt;��һҳ</a>&nbsp;&nbsp;&nbsp; ");
    sb.append("<a href='#' id='nextPage'>��һҳ&gt;&gt;</a>&nbsp;&nbsp;&nbsp; ");
    sb.append("<a href='#' id='lastPage'>ĩҳ&gt;&gt;|</a>&nbsp;&nbsp;&nbsp;");
    sb.append("��ǰ�� <label id='firPageLabel'>");
    sb.append(getCurrentPage() + "</label> ҳ/");
    sb.append("�� <label id='totalPageLabel'>" + getPageCount());
    sb.append("</label> ҳ&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
    sb.append("���� <input type='text' size='2' id='pageNum' onKeyPress='return event.keyCode>=48&&event.keyCode<=57'>ҳ ");
    sb.append("<a href='#' id='currentPage'>go&nbsp;&nbsp;&nbsp;</a> ");
    sb.append("<input name='pageSize' type='text' id='pageSize' ");
    sb.append("onKeyPress='return event.keyCode>=48&&event.keyCode<=57' ");
    sb.append("value='" + getPageSize());
    sb.append("' size='1' maxlength='3' /> ��/ҳ &nbsp;&nbsp;&nbsp;</span>");
    return sb.toString();
  }

  public int getRecordCount()
  {
    return this.recordCount;
  }

  public void setRecordCount(int recordCount)
  {
    this.recordCount = recordCount;
  }
}