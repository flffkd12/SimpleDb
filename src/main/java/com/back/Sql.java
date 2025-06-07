package com.back;

import com.back.simpleDb.SimpleDb;
import com.mysql.cj.log.Log;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

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

  public Sql appendIn(String sql, Object... bindParam) {
    String markerString = String.join(", ", Collections.nCopies(bindParam.length, "?"));
    sqlBuilder.append(sql.replace("?", markerString)).append("\n");
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
          row.put(meta.getColumnName(i), rs.getObject(i));
        }

        rows.add(row);
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return rows;
  }

  public LocalDateTime selectDatetime() {
    try (Connection conn = DriverManager
        .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ResultSet rs = ps.executeQuery()
    ) {
      if (rs.next()) {
        Timestamp ts = rs.getTimestamp(1);

        if (ts != null) {
          return ts.toLocalDateTime();
        }
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return null;
  }

  public Long selectLong() {
    try (Connection conn = DriverManager
        .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())
    ) {
      for (int i = 0; i < bindParams.size(); i++) {
        ps.setObject(i + 1, bindParams.get(i));
      }

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return 0L;
  }

  public List<Long> selectLongs() {
    List<Long> longList = new ArrayList<>();

    try (Connection conn = DriverManager
        .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())
    ) {
      for (int i = 0; i < bindParams.size(); i++) {
        ps.setObject(i + 1, bindParams.get(i));
      }

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          longList.add(rs.getLong(1));
        }
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return longList;
  }

  public String selectString() {
    try (Connection conn = DriverManager
        .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ResultSet rs = ps.executeQuery();
    ) {
      if (rs.next()) {
        return rs.getString(1);
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return null;
  }

  public boolean selectBoolean() {
    try (Connection conn = DriverManager
        .getConnection(simpleDb.getUrl(), simpleDb.getUser(), simpleDb.getPassword());
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ResultSet rs = ps.executeQuery();
    ) {
      if (rs.next()) {
        return rs.getBoolean(1);
      }
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return false;
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
