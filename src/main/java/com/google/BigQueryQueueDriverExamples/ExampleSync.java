package com.google.BigQueryQueueDriverExamples;

import com.google.BigQueryQueueDriver.BQQClient;
import com.google.BigQueryQueueDriver.BQQException;
import com.google.cloud.bigquery.QueryResult;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
        QueryResult response = c.blockingQuery(sql, true);
        Helpers.Color.println(Helpers.Color.GREEN, "DONE:");
        Helpers.Color.println(Helpers.Color.YELLOW,
            "\tSQL: " + sql.replace("\n", " "));
        Helpers.Color.println(Helpers.Color.YELLOW, "\tRows: " + response.getTotalRows());

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
