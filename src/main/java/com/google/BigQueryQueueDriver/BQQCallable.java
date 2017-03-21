package com.google.BigQueryQueueDriver;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

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
   * Builds a BQ Client either from ServiceAccount or using default credentials
   * @return a BQ client
   * @throws FileNotFoundException if no service account found in path
   * @throws IOException if failed to read service account
   */
  private BigQuery buildBQClient() throws FileNotFoundException, IOException {
    if (mServiceAccountPath == null || mServiceAccountPath.isEmpty()) {
      return BigQueryOptions.getDefaultInstance().getService();
    } else {
      return BigQueryOptions.newBuilder()
          .setProjectId(mProjectId)
          .setCredentials(
              ServiceAccountCredentials.fromStream(new FileInputStream(
                  mServiceAccountPath)))
          .build()
          .getService();
    }
  }
  
  /**
   * Executes the instance's BigQuery SQL Query.
   * @throws BQQException query fails
   * @throws InterruptedException thread pool closed / killed before query finishes
   * @throws IOException if no Service Account Found
   * @throws FileNotFoundException  if failed to read Service Account
   */
  @Override
  public QueryResult call() throws BQQException, InterruptedException, FileNotFoundException, IOException {
    BigQuery bigquery = buildBQClient();

    QueryResponse response = bigquery.query(mQueryRequest);

    while (!response.jobCompleted()) {
      Thread.sleep(500L);
      response = bigquery.getQueryResults(response.getJobId());
    }
    
    List<BigQueryError> executionErrors = response.getExecutionErrors();
    if (!executionErrors.isEmpty()) {
      throw new BQQException("BigQueryError", executionErrors);
    }
    
    QueryResult result = response.getResult();
    return result;
  }

}
