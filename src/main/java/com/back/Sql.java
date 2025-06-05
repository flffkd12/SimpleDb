package com.back;

import com.back.simpleDb.SimpleDb;
import java.sql.*;
import java.util.*;
import javax.xml.transform.Result;

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
    try (
        Connection conn = DriverManager
            .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString(),
            Statement.RETURN_GENERATED_KEYS)
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

  public Map<String, Object> selectRow() {
    Map<String, Object> row = new HashMap<>();

    try (
        Connection conn = DriverManager
            .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ResultSet rs = ps.executeQuery()
    ) {
      if (rs.next()) {
        ResultSetMetaData meta = rs.getMetaData();
        int colCnt = meta.getColumnCount();

        for (int i = 1; i <= colCnt; i++) {
          row.put(meta.getColumnName(i), rs.getObject(i));
        }
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return row;
  }

  public List<Map<String, Object>> selectRows() {
    List<Map<String, Object>> rows = new ArrayList<>();

    try (
        Connection conn = DriverManager
            .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ResultSet rs = ps.executeQuery()
    ) {
      ResultSetMetaData meta = rs.getMetaData();
      int colCnt = meta.getColumnCount();

      while (rs.next()) {
        Map<String, Object> row = new HashMap<>();

        for (int i = 1; i <= colCnt; i++) {
          String colName = meta.getColumnName(i);
          Object val = rs.getObject(colName);
          row.put(colName, val);
        }

        rows.add(row);
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return rows;
  }

  public int update() {
    try (
        Connection conn = DriverManager
            .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())
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
    try (Connection conn = DriverManager
        .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())
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
