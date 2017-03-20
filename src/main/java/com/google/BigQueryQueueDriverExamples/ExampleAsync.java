package com.google.BigQueryQueueDriverExamples;

import com.google.BigQueryQueueDriver.BQQClient;
import com.google.BigQueryQueueDriver.BQQException;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.QueryResult;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ExampleAsync {

  public static void run() {
    System.out.println("===============================");
    System.out.println("====== ASYNC Example ==========");
    
    // Creating Client that uses projectId strong-moose and given service account
    BQQClient c = new BQQClient("strong-moose", "/usr/local/google/home/bookman/service_account.json");

    // only allow at max 2 concurrent queries
    c.startup(2);
    
    // Queue up all my queries
    List<String> sqls = ExampleQueries.queries();
    HashMap<String, Future<QueryResult>> map = new HashMap<String, Future<QueryResult>>();
    for (String sql : sqls) {
      map.put(sql, c.queueQuery(sql, true));
    }
    
    // Block until all queries are done, and output info once a query has results
    while (!map.isEmpty()) {
      System.out.println("Number of queued up jobs: " + c.getNumJobs());
      try {
        Thread.sleep(500);
      } catch (InterruptedException e1) {
        // TODO(bookman): Auto-generated catch block
        e1.printStackTrace();
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
             QueryResult response = BQQClient.getQueryResultFuture(queryFuture);
             System.out.println("DONE:");
             System.out.println("\tSQL: " + entry.getKey().replace("\n", " "));
             System.out.println("\tRows: " + response.getTotalRows());
             // Uncomment if you'd like the response to be printed
             // Helpers.printRows(response);
             
           } catch (BQQException e) {
             for (BigQueryError bqerr : e.getBQErrors()) {
               System.err.println(bqerr.getMessage());
             }
             
           } catch (InterruptedException e) {
             System.out.println("Interrupted");
             Thread.currentThread().interrupt(); // ignore / reset
             
           } catch (ExecutionException e) {
             // unknown exception, simply printing it as a stacktrace for logging
             e.printStackTrace(); 
           }
         }
       }
     }
    try {
      c.teardown();
    } catch(Exception e) {
      System.out.println("Failed to teardown BQQClient threadpool");
      e.printStackTrace();
    }
  }
}
