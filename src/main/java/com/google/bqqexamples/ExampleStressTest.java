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
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Stress test to see N concurrent queries running at the same time
 */
public class ExampleStressTest {
  public static final int NUMBER_OF_CONCURRENT_QUERIES = 40;
  public static final int NUMBER_OF_TOTAL_QUERIES = 100;

  public static final String POPULAR_GOLANG_PACKAGES_SQL = "SELECT " 
      + "  REGEXP_EXTRACT(line, r'\"([^\"]+)\"') AS url, "
      + "  COUNT(*) AS count "
      + "FROM "
      + "  FLATTEN( ( "
      + "    SELECT "
      + "      SPLIT(SPLIT(REGEXP_EXTRACT("
      + "          content, r'.*import\\s*[(]([^)]*)[)]'),"
      + "          '\\n'), ';') AS line,"
      + "    FROM ( "
      + "      SELECT "
      + "        id, "
      + "        content "
      + "      FROM "
      + "        [bigquery-public-data:github_repos.sample_contents] "
      + "      WHERE "
      + "        REGEXP_MATCH(content, r'.*import\\s*[(][^)]*[)]')) AS C "
      + "    JOIN ( "
      + "      SELECT "
      + "        id "
      + "      FROM "
      + "        [bigquery-public-data:github_repos.sample_files] "
      + "      WHERE "
      + "        path LIKE '%.go' "
      + "      GROUP BY "
      + "        id) AS F "
      + "    ON "
      + "      C.id = F.id), line) "
      + "GROUP BY "
      + "  url "
      + "HAVING "
      + "  url IS NOT NULL "
      + "ORDER BY "
      + "  count DESC " 
      + "LIMIT 10 ";
    
  /**
   * Run the example.
   */
  public static void run() {
    BQQClient bqqClient = new BQQClient();
    try {
      bqqClient.startup(NUMBER_OF_CONCURRENT_QUERIES);
      
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
      
    ArrayList<Future<QueryResult>> futures = new ArrayList<>();
    for (int i = 0; i < NUMBER_OF_TOTAL_QUERIES; ++i) {
      Future<QueryResult> f = bqqClient.queueQuery(QueryRequest.newBuilder(POPULAR_GOLANG_PACKAGES_SQL)
          .setUseLegacySql(true)
          .setUseQueryCache(false)
          .build());
      futures.add(f);
    }
      
    while (!futures.isEmpty()) {
      System.out.println("Number of queries remaining: " + futures.size());

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      ArrayList<Future<QueryResult>> stillRunning = new ArrayList<>();
      for (Future<QueryResult> future : futures) {        
        if (!future.isDone()) {
          stillRunning.add(future);
          
        } else {            
          // This should not throw any errors
          QueryResult r;
          try {
            r = BQQClient.getQueryResult(future);
            if (r.getTotalRows() == 0) {
              System.out.println("No rows from output :(");
            }

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // ignore / reset

          } catch (BQQException e) {
            Helpers.printErrorCodes(e);
            
          } catch (ExecutionException e) {
            e.printStackTrace();
          } 
        }
      }
      
      futures = stillRunning;
    }
  }
}
