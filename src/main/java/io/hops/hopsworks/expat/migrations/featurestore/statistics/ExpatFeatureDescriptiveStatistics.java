package io.hops.hopsworks.expat.migrations.featurestore.statistics;

/**
 * This file is part of Expat
 * Copyright (C) 2024, Hopsworks AB. All rights reserved
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
 *
 */

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ExpatFeatureDescriptiveStatistics {
  public Integer id;
  public String featureType;
  public String featureName;
  
  // for any feature type
  public Long count;
  public Double completeness;
  public Long numNonNullValues;
  public Long numNullValues;
  public Long approxNumDistinctValues;
  
  // for numerical features
  public Double min;
  public Double max;
  public Double sum;
  public Double mean;
  public Double stddev;
  public List<Double> percentiles;
  
  // with exact uniqueness
  public Double distinctness;
  public Double entropy;
  public Double uniqueness;
  public Long exactNumDistinctValues;
  
  // histogram, correlations, kll <- from hdfs file
  public String extendedStatistics;
  
  public static Collection<ExpatFeatureDescriptiveStatistics> parseStatisticsJsonString(String content) {
    JSONArray columns = (new JSONObject(content)).getJSONArray("columns");
    HashMap<String, ExpatFeatureDescriptiveStatistics> descFdsMap = new HashMap<>();
    for (int i = 0; i < columns.length(); i++) {
      JSONObject colStats = (JSONObject) columns.get(i);
      ExpatFeatureDescriptiveStatistics fds = ExpatFeatureDescriptiveStatistics.fromJSON(colStats);
      descFdsMap.merge(fds.featureName, fds, (fds1, fds2) -> ExpatFeatureDescriptiveStatistics.merge(fds1, fds2));
    }
    return descFdsMap.values();
  }
  
  public static ExpatFeatureDescriptiveStatistics fromJSON(JSONObject statsJson) {
    ExpatFeatureDescriptiveStatistics fds = new ExpatFeatureDescriptiveStatistics();
    fds.featureName = statsJson.getString("column");
    
    if (statsJson.has("dataType")) {
      fds.featureType = statsJson.getString("dataType");
    }
    
    if (statsJson.has("count") && statsJson.getLong("count") == 0) {
      // if empty data, ignore the rest of statistics
      fds.count = 0L;
      return fds;
    }
    
    // common for all data types
    if (statsJson.has("numRecordsNull")) {
      fds.numNullValues = statsJson.getLong("numRecordsNull");
    }
    if (statsJson.has("numRecordsNonNull")) {
      fds.numNonNullValues = statsJson.getLong("numRecordsNonNull");
    }
    if (statsJson.has("numRecordsNull") && statsJson.has("numRecordsNonNull")) {
      fds.count = Long.valueOf(statsJson.getInt("numRecordsNull") + statsJson.getInt("numRecordsNonNull"));
    }
    if (statsJson.has("count")) {
      fds.count = statsJson.getLong("count");
    }
    if (statsJson.has("completeness")) {
      fds.completeness = statsJson.getDouble("completeness");
    }
    if (statsJson.has("approximateNumDistinctValues")) {
      fds.approxNumDistinctValues = statsJson.getLong("approximateNumDistinctValues");
    }
    
    // commmon for all data types if exact_uniqueness is enabled
    if (statsJson.has("uniqueness")) {
      fds.uniqueness = statsJson.getDouble("uniqueness");
    }
    if (statsJson.has("entropy")) {
      fds.entropy = statsJson.getDouble("entropy");
    }
    if (statsJson.has("distinctness")) {
      fds.distinctness = statsJson.getDouble("distinctness");
    }
    if (statsJson.has("exactNumDistinctValues")) {
      fds.exactNumDistinctValues = statsJson.getLong("exactNumDistinctValues");
    }
    
    // fractional / integral features
    if (statsJson.has("minimum")) {
      fds.min = statsJson.getDouble("minimum");
    }
    if (statsJson.has("maximum")) {
      fds.max = statsJson.getDouble("maximum");
    }
    if (statsJson.has("sum")) {
      fds.sum = statsJson.getDouble("sum");
    }
    if (statsJson.has("mean")) {
      fds.mean = statsJson.getDouble("mean");
    }
    if (statsJson.has("stdDev")) {
      fds.stddev = statsJson.getDouble("stdDev");
    }
    if (statsJson.has("percentiles")) {
      JSONArray percJsonArray = statsJson.getJSONArray("percentiles");
      fds.percentiles = new ArrayList<>();
      for (int i = 0; i < percJsonArray.length(); i++) {
        fds.percentiles.add(percJsonArray.getDouble(i));
      }
    }
    
    JSONObject extendedStatistics = new JSONObject();
    if (statsJson.has("correlations")) {
      extendedStatistics.put("correlations", statsJson.getJSONArray("correlations"));
    }
    if (statsJson.has("histogram")) {
      extendedStatistics.put("histogram", statsJson.getJSONArray("histogram"));
    }
    if (statsJson.has("kll")) {
      extendedStatistics.put("kll", statsJson.getJSONObject("kll"));
    }
    if (statsJson.has("unique_values")) {
      extendedStatistics.put("unique_values", statsJson.getJSONArray("unique_values"));
    }
    if (extendedStatistics.length() > 0) {
      fds.extendedStatistics = extendedStatistics.toString();
    }
    
    return fds;
  }
  
  public static ExpatFeatureDescriptiveStatistics merge(ExpatFeatureDescriptiveStatistics fds1,
    ExpatFeatureDescriptiveStatistics fds2) {
    // In legacy statistics, unique values are stored in a separate hdfs file, resulting in two files
    // for the same feature: one with descriptive statistics and the other one with unique values
    ExpatFeatureDescriptiveStatistics fds; // fds with statistics but no unique values
    ExpatFeatureDescriptiveStatistics fdsWithUV; // fds with unique values
    
    if (fds1.extendedStatistics != null && fds1.extendedStatistics.contains("\"unique_values\":")) {
      fds = fds2;
      fdsWithUV = fds1;
    } else {
      if (fds2.extendedStatistics != null && fds2.extendedStatistics.contains("\"unique_values\":")) {
        fds = fds1;
        fdsWithUV = fds2;
      } else {
        return fds1;  // otherwise, keep existing fds
      }
    }
    
    if (fds.extendedStatistics == null) {
      fds.extendedStatistics = fdsWithUV.extendedStatistics;
      return fds;
    }
    JSONObject fdsStats = new JSONObject(fds.extendedStatistics);
    JSONObject fdsWithUVStats = new JSONObject(fdsWithUV.extendedStatistics);
    // add unique_values
    fdsStats.put("unique_values", fdsWithUVStats.getJSONArray("unique_values"));
    fds.extendedStatistics = fdsStats.toString();
    return fds;
  }
}
