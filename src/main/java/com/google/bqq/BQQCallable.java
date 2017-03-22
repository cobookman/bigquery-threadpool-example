package com.google.bqq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Immutable Blocking BigQuery task to execute.
 */
public class BQQCallable implements Callable<QueryResult> {
  private String mProjectId;
  private String mServiceAccountPath = "";
  private QueryRequest mQueryRequest;

    
  /**
   * Generates a new BQQCallable instance
   * If serviceAccountPath is an empty string / null, then default credentials used instead.
   * @param projectId project that the bq client bills / auths against
   * @param serviceAccountPath absolute path to a service account, or null/"" for default creds.
   * @param queryRequest the Query that needs to be run
   */
  public BQQCallable(String projectId, String serviceAccountPath, 
      QueryRequest queryRequest) {
    mProjectId = projectId;
    mServiceAccountPath = serviceAccountPath;
    mQueryRequest = queryRequest;
  }
  
  /**
   * Executes the instance's BigQuery SQL Query.
   * @throws BQQException query fails
   * @throws InterruptedException thread pool closed / killed before query finishes
   * @throws IOException if no Service Account Found
   * @throws FileNotFoundException  if failed to read Service Account
   */
  @Override
  public QueryResult call() throws BQQException, InterruptedException,
    FileNotFoundException, IOException {
    
    BigQuery bigquery = BQQServiceFactory.buildClient(mProjectId, mServiceAccountPath);
    QueryResponse response;
    try {
      response = bigquery.query(mQueryRequest);
    } catch (BigQueryException e) {
      throw new BQQException("Query failed to be sent: " + mQueryRequest, e);
    }
    
    while (!response.jobCompleted()) {
      Thread.sleep(500L);
      try {
        response = bigquery.getQueryResults(response.getJobId());
      } catch (BigQueryException e) {
        throw new BQQException("Failed to grab query results", e);
      }
    }
    
    List<BigQueryError> executionErrors = response.getExecutionErrors();
    if (!executionErrors.isEmpty()) {
      throw new BQQException("BigQueryError", executionErrors);
    }
    
    QueryResult result = response.getResult();
    return result;
  }

}
