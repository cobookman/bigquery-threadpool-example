/*
 * Copyright 2017 Google Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.google.bqqexamples;

import com.google.bqq.BQQClient;
import com.google.bqq.BQQException;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Example of queuing up lots of queries, then blocking until they all finish.
 */
public class ExampleAsync {

  /**
   * run the example
   */
  public static void run() {
    System.out.println("Asynchronous Query Execution");


    // Creating Client that uses projectId strong-moose and given service account
    BQQClient bqqClient =
        new BQQClient("strong-moose", "/usr/local/google/home/bookman/service_account.json");

    // only allow at max 2 concurrent queries
    try {
      bqqClient.startup(2);

      // Catch errors involving bad service account path
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Queue up all my queries
    List<String> sqls = ExampleQueries.queries();
    HashMap<String, Future<QueryResult>> map = new HashMap<String, Future<QueryResult>>();
    for (String sql : sqls) {
      map.put(sql, bqqClient.queueQuery(sql, true));
    }

    // Queue up parameterized query
    int minWordCount = 10;
    String corpus = "tempest";
    String parameterizedSql = "SELECT word, word_count\n"
        + "FROM `bigquery-public-data.samples.shakespeare`\n" + "WHERE corpus = @corpus\n"
        + "AND word_count >= @min_word_count\n" + "ORDER BY word_count DESC";

    QueryRequest parameterizedQueryRequest = QueryRequest.newBuilder(parameterizedSql)
        .addNamedParameter("corpus", QueryParameterValue.string(corpus))
        .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
        .setUseLegacySql(false).build();
    map.put(parameterizedSql, bqqClient.queueQuery(parameterizedQueryRequest));

    // Block until all queries are done, and output info once a query has results
    while (!map.isEmpty()) {
      System.out.println("\tNumber of queued up jobs: " + bqqClient.getNumJobs());
      try {
        Thread.sleep(500);
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt(); // ignore / reset
      }

      // See what queries have finished
      Iterator<Map.Entry<String, Future<QueryResult>>> iter = map.entrySet().iterator();
      Entry<String, Future<QueryResult>> entry;
      while (iter.hasNext()) {
        entry = iter.next();
        Future<QueryResult> queryFuture = entry.getValue();

        // Query finished, grab result & remove from our queue
        if (queryFuture.isDone()) {
          iter.remove();

          // Get Query Results & print
          try {
            QueryResult response = BQQClient.getQueryResult(queryFuture);
            assert (response.getTotalRows() > 0);
            System.out.println("\tQuery Done");
            System.out.println("\t\tSql: " + entry.getKey().replace("\n", ""));
            System.out.println("\t\tRows: " + response.getTotalRows());
          } catch (BQQException e) {
            Helpers.printErrorCodes(e);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // ignore / reset

          } catch (ExecutionException e) {
            // unknown exception, simply printing it as a stacktrace for logging
            e.printStackTrace();
          }
        }
      }
    }

    try {
      bqqClient.shutdown();
    } catch (Exception e) {
      System.out.println("Failed to teardown BQQClient threadpool");
      e.printStackTrace();
    }
  }
}
