package com.back;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import lombok.Getter;

@Getter
public class Article {

  private int id;
  private LocalDateTime createdDate;
  private LocalDateTime modifiedDate;
  private String title;
  private String body;
  @JsonProperty("isBlind")
  private boolean isBlind;
}
