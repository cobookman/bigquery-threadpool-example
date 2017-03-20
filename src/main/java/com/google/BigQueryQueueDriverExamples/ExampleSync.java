package com.google.BigQueryQueueDriverExamples;

import com.google.BigQueryQueueDriver.BQQClient;
import com.google.BigQueryQueueDriver.BQQException;
import com.google.cloud.bigquery.QueryResult;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ExampleSync {
  public static void run() {
    System.out.println("===============================");
    System.out.println("======= SYNC Example ==========");
    
    // Creating Client that uses projectId strong-moose and given service account
    BQQClient c = new BQQClient("strong-moose", "/usr/local/google/home/bookman/service_account.json");
    
    // only allow at max 2 concurrent queries
    c.startup(2);
    
    List<String> sqls = ExampleQueries.queries();
    for (String sql : sqls) {
      try {
        QueryResult response = c.blockingQuery(sql, true);
        System.out.println("DONE:");
        System.out.println("\tSQL: " + sql.replace("\n", " "));
        System.out.println("\tRows: " + response.getTotalRows());
      } catch (BQQException | InterruptedException | ExecutionException e) {
        // TODO(bookman): Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    try {
      c.teardown();
    } catch (Exception e) {
      // TODO(bookman): Auto-generated catch block
      e.printStackTrace();
    }
  }
}
