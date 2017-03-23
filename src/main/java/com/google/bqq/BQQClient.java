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

package com.google.bqq;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A class to schedule BQQCallable tasks across n threads. It handles the
 * entire thread pool life-cycle from startup, scheduling, and shutdown.
 */
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
    if (projectId == null || projectId.isEmpty()) {
      throw new IllegalArgumentException("projectId is null / empty string");
    }
    
    if (serviceAccountPath == null || serviceAccountPath.isEmpty()) {
      throw new IllegalArgumentException("projectId is null / empty string");
    }
    
    mProjectId = projectId;
    mServiceAccountPath = serviceAccountPath;
  }
  
  /**
   * Starts up a thread pool to handle BQ SQL requests.
   * @param numThreads number of worker threads / max concurrent queries to handle requests 
   * @throws IOException thrown if failed to read service account
   * @throws FileNotFoundException thrown if no service account found in path specified 
   */
  public void startup(int numThreads) throws FileNotFoundException, IOException {   
    // Sanity check that our credentials are valid by creating a BQ client connection
    BQQServiceFactory.buildClient(mProjectId, mServiceAccountPath);

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
  public static QueryResult getQueryResult(Future<QueryResult> queryResultFuture) 
      throws InterruptedException, BQQException, ExecutionException {

    QueryResult result = null;
    try {
      result = queryResultFuture.get();

    } catch (ExecutionException e) { 
      
      Throwable t = e.getCause();
      if (t instanceof BQQException) {
        throw ((BQQException) t);
        
      } else if (t instanceof BigQueryException) {
        throw new BQQException((BigQueryException) t);
      } else {
        throw e;
      }
    } 
    return result;
  }
  
  /**
   * Tears down the underlying thread pool. With a default 100ms termination timeout.
   * @throws Exception error that occurs when tearing down thread pool.
   */
  public void shutdown() throws Exception {
    shutdown(1000);
  }
  
  /**
   * Tears down the underlying thread pool
   * @param terminationTimeout time to wait for thread pool to turn before timing out
   * @throws Exception error that occurs when tearing down thread pool.
   */
  public void shutdown(int terminationTimeout) throws Exception {
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
