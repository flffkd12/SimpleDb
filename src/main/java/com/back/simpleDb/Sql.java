package com.back.simpleDb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

public class Sql {

  private final Connection conn;
  private final StringBuilder sqlBuilder = new StringBuilder();
  private final List<Object> bindParams = new ArrayList<>();
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
  private final Logger logger = LoggerFactory.getLogger(Sql.class);

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
    Integer result = executeSql();
    return result != null ? result : 0;
  }

  public int delete() {
    Integer result = executeSql();
    return result != null ? result : 0;
  }

  private <T> T executeSql() {
    return executeSql(null, null);
  }

  private <T> T executeSql(Class<T> clazz) {
    return executeSql(clazz, null);
  }

  private <T, E> T executeSql(Class<T> clazz, Class<E> listType) {
    String sql = sqlBuilder.toString();

    try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
      bindParameters(ps);

      if (sql.startsWith("INSERT")) {
        ps.executeUpdate();
        try (ResultSet rs = ps.getGeneratedKeys()) {
          if (rs.next()) {
            return (T) (Long) rs.getLong(1);
          }
        }
      }

      if (sql.startsWith("SELECT")) {
        return parseResultSet(ps.executeQuery(), clazz, listType);
      }

      return (T) (Integer) ps.executeUpdate();
    } catch (SQLException e) {
      logger.error(e, () -> "SQL execution failed: %s, SQL: %s, clazz: %s, listType: %s"
          .formatted(e.getMessage(), sql, clazz, listType));
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private <T, E> T parseResultSet(
      ResultSet rs,
      Class<T> clazz,
      Class<E> listType
  ) throws SQLException {
    if (!rs.next())
      throw new NoSuchElementException("No result for select query");

    return switch (clazz.getSimpleName()) {
      case "Boolean" -> (T) (Boolean) rs.getBoolean(1);
      case "Long" -> (T) (Long) rs.getLong(1);
      case "String" -> (T) rs.getString(1);
      case "Map" -> (T) mapResultSet(rs);
      case "LocalDateTime" -> (T) rs.getTimestamp(1).toLocalDateTime();
      case "List" -> switch (listType.getSimpleName()) {
        case "Long" -> (T) getListFromResultSet(rs, resultSet -> resultSet.getLong(1));
        case "Map" -> (T) getListFromResultSet(rs, this::mapResultSet);
        default -> {
          IllegalStateException e = new IllegalStateException("Unexpected class value");
          logger.error(e, () ->
              "Unexpected listType value: %s".formatted(listType.getSimpleName()));
          throw e;
        }
      };
      default -> {
        IllegalStateException e = new IllegalStateException("Unexpected class value");
        logger.error(e, () -> "Unexpected clazz value: %s".formatted(clazz.getSimpleName()));
        throw e;
      }
    };
  }

  private <T> List<T> getListFromResultSet(
      ResultSet rs,
      SQLExceptionFunction<ResultSet, T> mapper
  ) throws SQLException {
    List<T> list = new ArrayList<>();
    do {
      list.add(mapper.apply(rs));
    } while (rs.next());
    return list;
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
