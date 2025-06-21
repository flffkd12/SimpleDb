package com.back;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {

  private final Connection conn;
  private final StringBuilder sqlBuilder = new StringBuilder();
  private final List<Object> bindParams = new ArrayList<>();

  public Sql(Connection conn) {
    this.conn = conn;
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
        PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString(),
            Statement.RETURN_GENERATED_KEYS)
    ) {
      bindParameters(ps);
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

  public <T> T selectRow(Class<T> clazz) {
    Map<String, Object> row = selectRow();
    return new ObjectMapper().registerModule(new JavaTimeModule()).convertValue(row, clazz);
  }

  public List<Map<String, Object>> selectRows() {
    List<Map<String, Object>> rows = new ArrayList<>();

    try (
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

  public <T> List<T> selectRows(Class<T> clazz) {
    ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    List<Map<String, Object>> rows = selectRows();
    List<T> itemList = new ArrayList<>();

    for (Map<String, Object> row : rows) {
      itemList.add(mapper.convertValue(row, clazz));
    }

    return itemList;
  }

  public LocalDateTime selectDatetime() {
    try (
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
    try (PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())) {
      bindParameters(ps);

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

    try (PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())) {
      bindParameters(ps);
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
    try (
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
    try (
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
    return executeSql();
  }

  public int delete() {
    return executeSql();
  }

  private int executeSql() {
    try (PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())) {
      bindParameters(ps);
      ps.executeUpdate();

      return ps.getUpdateCount();
    } catch (SQLTimeoutException e) {

    } catch (SQLException e) {

    }

    return -1;
  }

  private void bindParameters(PreparedStatement ps) throws SQLException {
    for (int i = 0; i < bindParams.size(); i++) {
      ps.setObject(i + 1, bindParams.get(i));
    }
  }

}
