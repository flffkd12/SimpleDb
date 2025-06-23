package com.back.simpleDb;

import com.back.Sql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

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
  private final Logger logger = LoggerFactory.getLogger(SimpleDb.class);

  public SimpleDb(String host, String user, String password, String dbName) {
    this.url = "jdbc:mysql://" + host + ":3307/" + dbName
        + "?serverTimezone=Asia/Seoul&characterEncoding=utf8";
    this.user = user;
    this.password = password;
  }

  public Connection getConnection() {
    Connection conn = threadLocalConn.get();

    if (conn != null)
      return conn;

    try {
      conn = DriverManager.getConnection(url, user, password);
      threadLocalConn.set(conn);
      return conn;
    } catch (SQLException e) {
      logger.error(() -> "DB connection failed: %s, url: %s, user: %s"
          .formatted(e.getMessage(), url, user));
      throw new RuntimeException("DB connection failed", e);
    }
  }

  public void run(String sql, Object... params) {
    Connection conn = getConnection();

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }

      ps.executeUpdate();
    } catch (SQLException e) {
      logger.error(e, () -> "SQL execution failed: %s, SQL: %s, bindParams: %s"
          .formatted(e.getMessage(), sql, Arrays.toString(params)));
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  public Sql genSql() {
    return new Sql(getConnection());
  }

  public void close() {
    Connection conn = threadLocalConn.get();

    if (conn == null)
      return;

    try {
      conn.close();
      threadLocalConn.remove();
    } catch (SQLException e) {
      logger.error(e, () -> "Failed to close DB connection: %s".formatted(e.getMessage()));
    }
  }

  public void startTransaction() {
    if (isInTransaction)
      return;

    Connection conn = getConnection();

    try {
      conn.setAutoCommit(false);
      isInTransaction = true;
    } catch (SQLException e) {
      logger.error(e, () -> "Failed to set auto commit: %s".formatted(e.getMessage()));
      throw new RuntimeException("Failed to start transaction", e);
    }
  }

  public void commit() {
    Connection conn = getConnection();

    try {
      conn.commit();
      conn.setAutoCommit(true);
      isInTransaction = false;
    } catch (SQLException e) {
      logger.error(e, () -> "Failed to commit: %s".formatted(e.getMessage()));
      throw new RuntimeException("Failed to commit", e);
    }
  }

  public void rollback() {
    Connection conn = getConnection();

    try {
      conn.rollback();
      conn.setAutoCommit(true);
      isInTransaction = false;
    } catch (SQLException e) {
      logger.error(e, () -> "Failed to rollback: %s".formatted(e.getMessage()));
      throw new RuntimeException("Failed to commit", e);
    }
  }
}
