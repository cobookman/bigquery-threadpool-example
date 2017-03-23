/*
  Copyright 2017 Google Inc.
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.google.bqqexamples;

import com.google.bqq.BQQClient;
import com.google.bqq.BQQException;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Synchronous blocking example.
 */
public class ExampleSync {
  
  /**
   * Run the example.
   */
  public static void run() {
    System.out.println("Synchronous Query Execution");
    
    BQQClient bqqClient = new BQQClient();
    
    // Startup 2 worker threads to handle bq queries, allowing only
    // 2 concurrent queries at any time.
    try {
      bqqClient.startup(2);
      
    // Catch errors involving bad service account path
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Run only 1 query at a time, and block thread until results are in
    List<String> sqls = ExampleQueries.queries();
    for (String sql : sqls) {
      Future<QueryResult> queryFuture = bqqClient.queueQuery(sql, true);
      
      try {
        QueryResult response = BQQClient.getQueryResult(queryFuture);
        assert(response.getTotalRows() > 0);

        System.out.println("\tQuery Done:");
        System.out.println("\t\tSql: " + sql.replace("\n", ""));
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
    
    // User input based query!
    int minWordCount = 10;
    String corpus = "tempest";
    String parameterizedSql = "SELECT word, word_count\n"
        + "FROM `bigquery-public-data.samples.shakespeare`\n"
        + "WHERE corpus = @corpus\n"
        + "AND word_count >= @min_word_count\n"
        + "ORDER BY word_count DESC";
    
    QueryRequest parameterizedQueryRequest = QueryRequest
        .newBuilder(parameterizedSql)
        .addNamedParameter("corpus", QueryParameterValue.string(corpus))
        .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
        .setUseLegacySql(false)
        .build();
    Future<QueryResult> userInputQueryResponse = bqqClient.queueQuery(parameterizedQueryRequest);
    
    try {
      // Block and wait until future resolves
      QueryResult userInputQueryResult = BQQClient.getQueryResult(userInputQueryResponse);
      assert(userInputQueryResult.getTotalRows() > 0);

      System.out.println("\tQuery Done:");
      System.out.println("\t\tSql: " + parameterizedSql.replace("\n", ""));
      System.out.println("\t\tRows: " + userInputQueryResult.getTotalRows());

    } catch (BQQException e) {
      Helpers.printErrorCodes(e);
      
    } catch (InterruptedException | ExecutionException e) {
      // handle exception in running query
      e.printStackTrace();
    }
    
    // Teardown BQ threadpool
    try {
      bqqClient.shutdown();
      
    } catch (Exception e) {
      // Handle exception in tearing down threadpool
      e.printStackTrace();
    }
  }
}
