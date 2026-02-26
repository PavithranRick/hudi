/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test Spark read on an existing Hudi table (e.g. after migration).
 * Set system property {@code hudi.test.migration.table.path} to the table base path to run
 * against your local table; otherwise uses the default path below.
 */
public class TestMigrationSparkRead extends HoodieSparkClientTestBase {

  /**
   * Default path for local runs. Override with -Dhudi.test.migration.table.path=/your/path
   */
  private static final String DEFAULT_BASE_PATH =
      "/Users/pavithran/local_testing/tests/timestamp_test/hudi_table_3.4_0.15.0_to_0_16_0_SNAPSHOT_MERGE_ON_READ_compaction_false/";

  private static String getBasePath() {
    String path = System.getProperty("hudi.test.migration.table.path");
    return path != null && !path.isEmpty() ? path : DEFAULT_BASE_PATH;
  }

  @Test
  public void testSparkRead() {
    Map<String, String> options = new HashMap<>();
    options.put(HoodieMetadataConfig.ENABLE.key(), "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    options.put(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "true");

    String basePath = getBasePath();

    // Test data skipping
    Dataset<Row> df1 = sparkSession.read().format("hudi")
        .options(options).load(basePath)
        .where("ts_millis >= '2026-01-23 05:36:53.417'");
    df1.select("id", "event_name", "partition", "ts_millis_long", "ts_millis", "local_ts_millis")
        .show(false);

    // Get actual commit times from the table
    List<String> commits = sparkSession.read().format("hudi")
        .options(options).load(basePath)
        .select("_hoodie_commit_time")
        .distinct()
        .orderBy("_hoodie_commit_time")
        .toJavaRDD()
        .map(row -> row.getString(0))
        .collect();

    if (commits.isEmpty()) {
      System.out.println("No commits found in the table. Skipping incremental and time travel queries.");
      return;
    }

    System.out.println("Found commits: " + commits);
    String beginTime = "000";
    String endTime = commits.get(commits.size() - 2);
    String timeTravelInstant = commits.get(commits.size() - 2);

    // Test incremental query.
    Dataset<Row> df2 = sparkSession.read().format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", beginTime)
        .option("hoodie.datasource.read.end.instanttime", endTime)
        .options(options)
        .load(basePath);
    System.out.println("Incremental query results (begin=" + beginTime + ", end=" + endTime + "):");
    df2.select("id", "event_name", "partition", "ts_millis_long", "ts_millis", "local_ts_millis")
        .show(false);

    // Test time travel.
    Dataset<Row> df3 = sparkSession.read().format("hudi")
        .option("as.of.instant", timeTravelInstant)
        .options(options)
        .load(basePath);
    System.out.println("Time travel query results (as.of.instant=" + timeTravelInstant + "):");
    df3.select("id", "event_name", "partition", "ts_millis_long", "ts_millis", "local_ts_millis")
        .show(false);
  }
}
