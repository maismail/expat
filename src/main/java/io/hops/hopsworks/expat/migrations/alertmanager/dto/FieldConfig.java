/*
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.expat.migrations.alertmanager.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "title",
  "value",
  "short"
  })
public class FieldConfig {
  @JsonProperty("title")
  private String title;
  @JsonProperty("value")
  private String value;
  @JsonProperty("short")
  private Boolean _short;
  
  public FieldConfig() {
  }
  
  public FieldConfig(String title, String value) {
    this.title = title;
    this.value = value;
  }
  
  @JsonProperty("title")
  public String getTitle() {
    return title;
  }
  
  @JsonProperty("title")
  public void setTitle(String title) {
    this.title = title;
  }
  
  @JsonProperty("value")
  public String getValue() {
    return value;
  }
  
  @JsonProperty("value")
  public void setValue(String value) {
    this.value = value;
  }
  
  @JsonProperty("short")
  public Boolean getShort() {
    return _short;
  }
  
  @JsonProperty("short")
  public void setShort(Boolean _short) {
    this._short = _short;
  }
}
