package com.back.simpleDb;

import com.back.Sql;
import java.sql.*;
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

  public SimpleDb(String host, String user, String password, String dbName) {
    this.url = "jdbc:mysql://" + host + ":3307/" + dbName
        + "?serverTimezone=Asia/Seoul&characterEncoding=utf8";
    this.user = user;
    this.password = password;
  }

  public void run(String sql, Object... params) {
    try (Connection conn = DriverManager.getConnection(url, user, password);
        PreparedStatement ps = conn.prepareStatement(sql)) {
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
    return new Sql(this);
  }
}
