package com.google.bqqexamples;

import com.google.bqq.BQQClient;
import com.google.bqq.BQQException;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Stress test to see N concurrent queries running at the same time
 */
public class ExampleStressTest {
  public static final int numberOfConcurrentQueries = 40;
  public static final int numberOfQueriesToBeRun = 100;
  

  public static final String popularGolangPackagesSQL = "SELECT\n" + 
      "  REGEXP_EXTRACT(line, r'\"([^\"]+)\"') AS url, " + 
      "  COUNT(*) AS count " + 
      "FROM " + 
      "  FLATTEN( ( " + 
      "    SELECT " + 
      "      SPLIT(SPLIT(REGEXP_EXTRACT(content, r'.*import\\s*[(]([^)]*)[)]'), '\\n'), ';') AS line," + 
      "    FROM ( " + 
      "      SELECT " + 
      "        id, " + 
      "        content " + 
      "      FROM " + 
      "        [bigquery-public-data:github_repos.sample_contents] " + 
      "      WHERE " + 
      "        REGEXP_MATCH(content, r'.*import\\s*[(][^)]*[)]')) AS C " + 
      "    JOIN ( " + 
      "      SELECT " + 
      "        id " + 
      "      FROM " + 
      "        [bigquery-public-data:github_repos.sample_files] " + 
      "      WHERE " + 
      "        path LIKE '%.go' " + 
      "      GROUP BY " + 
      "        id) AS F " + 
      "    ON " + 
      "      C.id = F.id), line) " + 
      "GROUP BY " + 
      "  url " + 
      "HAVING " + 
      "  url IS NOT NULL " + 
      "ORDER BY " + 
      "  count DESC " + 
      "LIMIT 10 ";
    
  public static void run() {
    BQQClient c = new BQQClient();
    try {
      c.startup(numberOfConcurrentQueries);
    } catch (FileNotFoundException e1) {
      // TODO(bookman): Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e1) {
      // TODO(bookman): Auto-generated catch block
      e1.printStackTrace();
    }
      
    ArrayList<Future<QueryResult>> futures = new ArrayList<>();
    for (int i = 0; i < numberOfQueriesToBeRun; ++i) {
      Future<QueryResult> f = c.queueQuery(QueryRequest.newBuilder(popularGolangPackagesSQL)
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
      for(Future<QueryResult> future : futures) {        
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
