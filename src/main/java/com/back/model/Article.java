package com.back.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import lombok.Getter;

@Getter
public class Article {

  private long id;
  private LocalDateTime createdDate;
  private LocalDateTime modifiedDate;
  private String title;
  private String body;
  @JsonProperty("isBlind")
  private boolean isBlind;
}
