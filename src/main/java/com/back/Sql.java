package com.back;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Sql {

  private final Connection conn;
  private final StringBuilder sqlBuilder = new StringBuilder();
  private final List<Object> bindParams = new ArrayList<>();
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

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
    return executeSql(Long.class);
  }

  public Map<String, Object> selectRow() {
    return executeSql(Map.class);
  }

  public <T> T selectRow(Class<T> clazz) {
    return objectMapper.convertValue(selectRow(), clazz);
  }

  public List<Map<String, Object>> selectRows() {
    return executeSql(List.class, Map.class);
  }

  public <T> List<T> selectRows(Class<T> clazz) {
    return selectRows().stream()
        .map(row -> objectMapper.convertValue(row, clazz))
        .collect(Collectors.toList());
  }

  public LocalDateTime selectDatetime() {
    return executeSql(LocalDateTime.class);
  }

  public Long selectLong() {
    Long result = executeSql(Long.class);
    return result != null ? result : 0L;
  }

  public List<Long> selectLongs() {
    return executeSql(List.class, Long.class);
  }

  public String selectString() {
    return executeSql(String.class);
  }

  public Boolean selectBoolean() {
    return executeSql(Boolean.class);
  }

  public int update() {
    Integer result = executeSql(Integer.class);
    return result != null ? result : 0;
  }

  public int delete() {
    Integer result = executeSql(Integer.class);
    return result != null ? result : 0;
  }

  private <T> T executeSql(Class<T> clazz) {
    return executeSql(clazz, null);
  }

  private <T, E> T executeSql(Class<T> clazz, Class<E> listType) {
    String sql = sqlBuilder.toString();

    if (sql.startsWith("INSERT")) {
      try (
          PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString(),
              Statement.RETURN_GENERATED_KEYS)
      ) {
        bindParameters(ps);
        ps.executeUpdate();

        try (ResultSet rs = ps.getGeneratedKeys()) {
          if (rs.next()) {
            return (T) (Long) rs.getLong(1);
          }
        }
      } catch (SQLFeatureNotSupportedException e) {
        return (T) (Long) 0L;
      } catch (SQLTimeoutException e) {
        return (T) (Long) 0L;
      } catch (SQLException e) {
        return (T) (Long) 0L;
      }
    }

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      bindParameters(ps);

      if (sql.startsWith("SELECT")) {
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
          if (clazz == Boolean.class) {
            return (T) (Boolean) rs.getBoolean(1);
          } else if (clazz == String.class) {
            return (T) rs.getString(1);
          } else if (clazz == Long.class) {
            return (T) (Long) rs.getLong(1);
          } else if (clazz == LocalDateTime.class) {
            Timestamp ts = rs.getTimestamp(1);

            if (ts != null) {
              return (T) ts.toLocalDateTime();
            }
          } else if (clazz == Map.class) {
            return (T) mapResultSet(rs);
          } else if (clazz == List.class) {
            if (listType == Long.class) {
              List<Long> longList = new ArrayList<>();

              do {
                longList.add(rs.getLong(1));
              } while (rs.next());
              return (T) longList;
            } else if (listType == Map.class) {
              List<Map<String, Object>> rows = new ArrayList<>();

              do {
                rows.add(mapResultSet(rs));
              } while (rs.next());

              return (T) rows;
            }
          }
        }
      }

      return (T) (Integer) ps.executeUpdate();
    } catch (SQLTimeoutException e) {
      return null;
    } catch (SQLException e) {
      return null;
    }
  }

  private Map<String, Object> mapResultSet(ResultSet rs) throws SQLException {
    Map<String, Object> row = new HashMap<>();
    ResultSetMetaData meta = rs.getMetaData();
    int colCnt = meta.getColumnCount();

    for (int i = 1; i <= colCnt; i++) {
      row.put(meta.getColumnName(i), rs.getObject(i));
    }

    return row;
  }

  private void bindParameters(PreparedStatement ps) throws SQLException {
    for (int i = 0; i < bindParams.size(); i++) {
      ps.setObject(i + 1, bindParams.get(i));
    }
  }
}
