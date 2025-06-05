package com.back;

import com.back.simpleDb.SimpleDb;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sql {

  private final SimpleDb simpleDb;
  private final StringBuilder sqlBuilder = new StringBuilder();
  private final List<Object> bindParams = new ArrayList<>();

  public Sql(SimpleDb simpleDb) {
    this.simpleDb = simpleDb;
  }

  public Sql append(String sql, Object... bindParam) {
    sqlBuilder.append(sql).append("\n");
    bindParams.addAll(Arrays.asList(bindParam));
    return this;
  }

  public long insert() {
    String sql = sqlBuilder.toString();
    try (
        Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUser(),
            simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    ) {
      for (int i = 0; i < bindParams.size(); i++) {
        ps.setObject(i + 1, bindParams.get(i));
      }

      ps.executeUpdate();

      try (ResultSet rs = ps.getGeneratedKeys()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
      }
    } catch (SQLFeatureNotSupportedException e) {

    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return 0;
  }

  public int update() {
    String sql = sqlBuilder.toString();
    try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUser(),
        simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sql)
    ) {
      for (int i = 0; i < bindParams.size(); i++) {
        ps.setObject(i + 1, bindParams.get(i));
      }

      ps.executeUpdate();
      return ps.getUpdateCount();
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return -1;
  }

  public int delete() {
    String sql = sqlBuilder.toString();
    try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUser(),
        simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sql)
    ) {
      for (int i = 0; i < bindParams.size(); i++) {
        ps.setObject(i + 1, bindParams.get(i));
      }

      ps.executeUpdate();
      return ps.getUpdateCount();
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return -1;
  }
}
