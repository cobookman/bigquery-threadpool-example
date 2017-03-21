package com.google.BigQueryQueueDriverExamples;

import com.google.BigQueryQueueDriver.BQQClient;
import com.google.BigQueryQueueDriver.BQQException;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ExampleSync {
  public static void run() {
    Helpers.Color.println(Helpers.Color.CYAN,
        "===============================");
    Helpers.Color.println(Helpers.Color.CYAN, 
        "====== SYNC Example ==========");
    
    // Creating Client that uses projectId strong-moose and given service account
    BQQClient c = new BQQClient("strong-moose", "/usr/local/google/home/bookman/service_account.json");
    
    // only allow at max 2 concurrent queries
    c.startup(2);
    
    List<String> sqls = ExampleQueries.queries();
    for (String sql : sqls) {
      try {
        Future<QueryResult> responseFuture = c.queueQuery(sql, true);
        QueryResult response = BQQClient.getQueryResultFuture(responseFuture);

        Helpers.Color.println(Helpers.Color.GREEN, "DONE:");
        Helpers.Color.println(Helpers.Color.YELLOW,
            "\tSQL: " + sql.replace("\n", " "));
        Helpers.Color.println(Helpers.Color.YELLOW, "\tRows: " + response.getTotalRows());

      } catch (BQQException | InterruptedException | ExecutionException e) {
        // TODO(bookman): Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    // User input based query!
    String corpus = "corpus";
    Integer minWordCount = 10;
    Future<QueryResult> userInputQueryResponse = c.queueQuery(QueryRequest
        .newBuilder(
            "SELECT word, word_count\n"
                + "FROM `bigquery-public-data.samples.shakespeare`\n"
                + "WHERE corpus = @corpus\n"
                + "AND word_count >= @min_word_count\n"
                + "ORDER BY word_count DESC")
        .addNamedParameter("corpus", QueryParameterValue.string(corpus))
        .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
        .setUseLegacySql(false)
        .build());
    
    try {
      QueryResult userInputQueryResult = BQQClient.getQueryResultFuture(userInputQueryResponse);
      Helpers.Color.println(Helpers.Color.GREEN, "DONE:");
      Helpers.Color.println(Helpers.Color.YELLOW, "\tRows: " + userInputQueryResult.getTotalRows());

    } catch (InterruptedException | BQQException | ExecutionException e1) {
      // TODO(bookman): Auto-generated catch block
      e1.printStackTrace();
    }
    
    try {
      c.teardown();
    } catch (Exception e) {
      // TODO(bookman): Auto-generated catch block
      e.printStackTrace();
    }
  }
}
