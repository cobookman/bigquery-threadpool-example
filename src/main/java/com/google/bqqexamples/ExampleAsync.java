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

public class ExampleAsync {

  public static void run() {
    System.out.println("Asynchronous Query Execution");


    // Creating Client that uses projectId strong-moose and given service account
    BQQClient c =
        new BQQClient("strong-moose", "/usr/local/google/home/bookman/service_account.json");

    // only allow at max 2 concurrent queries
    try {
      c.startup(2);

      // Catch errors involving bad service account path
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Queue up all my queries
    List<String> sqls = ExampleQueries.queries();
    HashMap<String, Future<QueryResult>> map = new HashMap<String, Future<QueryResult>>();
    for (String sql : sqls) {
      map.put(sql, c.queueQuery(sql, true));
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
    map.put(parameterizedSql, c.queueQuery(parameterizedQueryRequest));

    // Block until all queries are done, and output info once a query has results
    while (!map.isEmpty()) {
      System.out.println("\tNumber of queued up jobs: " + c.getNumJobs());
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
      c.shutdown();
    } catch (Exception e) {
      System.out.println("Failed to teardown BQQClient threadpool");
      e.printStackTrace();
    }
  }
}
