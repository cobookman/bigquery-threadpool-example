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
import java.util.concurrent.atomic.AtomicInteger;

public class BQQCallable implements Callable<QueryResult> {
  private String mProjectId;
  private String mQuery;
  private String mServiceAccountPath = "";
  private Boolean mUseLegacySQL;

  /**
   * Generates a new BQQCallable instance
   * @param projectId
   * @param query
   * @param serviceAccountPath
   * @param useLegacySQL
   */
  public BQQCallable(String projectId, String query,
      String serviceAccountPath, Boolean useLegacySQL) {
    mProjectId = projectId;
    mQuery = query;
    mServiceAccountPath = serviceAccountPath;
    mUseLegacySQL = useLegacySQL;
  }
  
  /**
   * Builds a BQ Client either from ServiceAccount or using default credentials
   * @return a BQ client
   * @throws FileNotFoundException if no service account found in path
   * @throws IOException if failed to read service account
   */
  private BigQuery buildBQClient() throws FileNotFoundException, IOException {
    if (mServiceAccountPath.isEmpty()) {
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

    QueryRequest queryRequest = QueryRequest
        .newBuilder(mQuery)
        .setUseLegacySql(mUseLegacySQL).build();
    
    QueryResponse response = bigquery.query(queryRequest);

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
