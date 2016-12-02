package com.turk.db.dao;

import java.util.List;

public abstract interface DAO<T>
{
  public abstract int add(T paramT);

  public abstract boolean update(T paramT);

  public abstract boolean delete(T paramT);

  public abstract boolean delete(int paramInt);

  public abstract List<T> list();

  public abstract T getById(int paramInt);

  public abstract T getByName(String paramString);

  public abstract List<T> criteriaQuery(T paramT);

  public abstract List<T> query(String paramString);

  public abstract boolean exists(T paramT);

  public abstract boolean validate(T paramT);

  public abstract PageQueryResult<T> pageQuery(int paramInt1, int paramInt2);
}