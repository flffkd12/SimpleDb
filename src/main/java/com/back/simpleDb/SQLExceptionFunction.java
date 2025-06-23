package com.back.simpleDb;

import java.sql.SQLException;

@FunctionalInterface
public interface SQLExceptionFunction<T, R> {

  R apply(T t) throws SQLException;
}