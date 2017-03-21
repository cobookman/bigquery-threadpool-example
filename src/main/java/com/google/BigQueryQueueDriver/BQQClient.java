package com.google.BigQueryQueueDriver;

import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BQQClient {  
  private String mProjectId;
  private String mServiceAccountPath;
  private ThreadPoolExecutor mPool;

  /**
   * Instantiates a Big Query Queue using default credentials
   */
  public BQQClient() {}
  
  /**
   * Instantiates a Big Query Queue w/auth by Service Account
   * @param projectId project to use for authentication & billing
   * @param serviceAccountPath path to a service account
   */
  public BQQClient(String projectId, String serviceAccountPath) {
    mProjectId = projectId;
    mServiceAccountPath = serviceAccountPath;
  }
  
  /**
   * Starts up a thread pool to handle BQ SQL requests.
   * @param numThreads number of worker threads / max concurrent queries to handle requests 
   */
  public void startup(int numThreads) {    
    // FIFO Queue
    BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<Runnable>();
    
    // ThreadPool of numThread workers which kills all threads that have done no work in 1000ms
    mPool = new ThreadPoolExecutor(numThreads, numThreads,
        1000, TimeUnit.MILLISECONDS, blockingQueue);
  }
  
  public Integer getNumJobs() {
    return mPool.getQueue().size();
  }
  
  /**
   * Queues up a QueryRequest to be executed on one of the thread pool threads.
   * @param queryRequest a QueryRequest to be queued up
   * @return a future with query results
   */
  public Future<QueryResult> queueQuery(QueryRequest queryRequest) {
    BQQCallable c = new BQQCallableBuilder()
        .setProjectId(mProjectId)
        .setServiceAccountPath(mServiceAccountPath)
        .setQueryRequest(queryRequest)
        .build();
    return mPool.submit(c);
  }
  
  /**
   * Queues up a query to be executed on one of the thread pool threads.
   * @param query SQL statement to execute
   * @param useLegacySql if the query is using Legacy SQL or not
   * @return a future with the query results
   */
  public Future<QueryResult> queueQuery(String query, boolean useLegacySql) {
    QueryRequest queryRequest = QueryRequest.newBuilder(query)
        .setUseLegacySql(useLegacySql)
        .build();

    return queueQuery(queryRequest);
  }
  
  /**
   * Resolves a Future<QueryResult> from queueQuery
   * @param queryResultFuture a QueryResult future
   * @return the QueryResult from Future<QueryResult>
   * @throws InterruptedException if query is interrupted
   * @throws BQQException if BigQuery had an error
   * @throws ExecutionException if the exception thrown by worker thread is of unknown type
   */
  public static QueryResult getQueryResultFuture(Future<QueryResult> queryResultFuture) throws InterruptedException, BQQException, ExecutionException {
    QueryResult result = null;
    try {
      result = queryResultFuture.get();
      
    } catch (ExecutionException e) { 
      
      Throwable t = e.getCause();
      if (t instanceof BQQException) {
        throw ((BQQException) t);
        
      } else {
        throw e;
      }
    } 
    return result;
  }
  
  /**
   * Queues up a query, then blocks until results are in
   * @param sql SQL statement to execute
   * @param useLegacySql if the query is using Legacy SQL or not
   * @return the query results A query result
   * @throws BQQException  the list of BigQuery errors
   * @throws InterruptedException if query is interrupted
   * @throws ExecutionException if the exception thrown in worker thread is unknown
   */
  public QueryResult blockingQuery(String sql, boolean useLegacySql) throws BQQException, InterruptedException, ExecutionException {
    Future<QueryResult> queryResultFuture = queueQuery(sql, useLegacySql);
    return getQueryResultFuture(queryResultFuture);
  }
  
  
  /**
   * Tears down the underlying thread pool. With a default 100ms termination timeout.
   * @throws Exception error that occurs when tearing down thread pool.
   */
  public void teardown() throws Exception {
    teardown(1000);
  }
  
  /**
   * Tears down the underlying thread pool
   * @param terminationTimeout time to wait for thread pool to turn before timing out
   * @throws Exception error that occurs when tearing down thread pool.
   */
  public void teardown(int terminationTimeout) throws Exception {
    // gracefully shutdown thread pool
    mPool.shutdown();
    
    try {
      // Check if thread pool terminated.
      // If after 'timeout' then force a shutdown.
      if (!mPool.awaitTermination(terminationTimeout, TimeUnit.MILLISECONDS)) {
        mPool.shutdownNow();
        
        // Check if forced shutdown was successful.
        if (!mPool.awaitTermination(terminationTimeout, TimeUnit.MILLISECONDS)) {
          throw new Exception("BQQClient.mPool will not terminate");
        }
      }
    } catch (InterruptedException e) {
      mPool.shutdownNow();
      
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

}
