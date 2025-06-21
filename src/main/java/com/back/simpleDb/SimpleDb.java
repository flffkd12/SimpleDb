package com.back.simpleDb;

import com.back.Sql;
import java.sql.*;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.Setter;

// SimpleDb에 관한 기본 정보를 담고 초기화해놓는 클래스
public class SimpleDb {

  @Getter
  private final String url;
  @Getter
  private final String user;
  @Getter
  private final String password;
  @Setter
  private boolean devMode = false;
  private boolean isInTransaction = false;
  private final ThreadLocal<Connection> threadLocalConn = new ThreadLocal<>();

  public SimpleDb(String host, String user, String password, String dbName) {
    this.url = "jdbc:mysql://" + host + ":3307/" + dbName
        + "?serverTimezone=Asia/Seoul&characterEncoding=utf8";
    this.user = user;
    this.password = password;
  }

  public Connection getConnection() {
    Connection conn = threadLocalConn.get();

    if (conn == null) {
      try {
        conn = DriverManager.getConnection(url, user, password);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      threadLocalConn.set(conn);
    }

    return conn;
  }

  public void run(String sql, Object... params) {
    Connection conn = getConnection();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }

      ps.executeUpdate();
    } catch (SQLTimeoutException e) {
      throw new RuntimeException(e);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Sql genSql() {
    return new Sql(getConnection());
  }

  public void close() {
    Connection conn = threadLocalConn.get();

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }

      threadLocalConn.remove();
    }
  }

  public void startTransaction() {
    if (isInTransaction) {
      return;
    }

    Connection conn = getConnection();

    try {
      conn.setAutoCommit(false);
      isInTransaction = true;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void commit() {
    Connection conn = getConnection();

    try {
      conn.commit();
      conn.setAutoCommit(true);
      isInTransaction = false;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void rollback() {
    Connection conn = getConnection();

    try {
      conn.rollback();
      conn.setAutoCommit(true);
      isInTransaction = false;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
