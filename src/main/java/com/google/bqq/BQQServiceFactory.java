package com.google.bqq;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A factory to build BigQuery Client connections.
 */
public final class BQQServiceFactory {
  
  public static BigQuery buildClient(String projectId, String serviceAccountPath)
      throws FileNotFoundException, IOException {
    
    boolean isProjectIdSet = projectId != null && !projectId.isEmpty();
    boolean isServiceAccountPathSet = serviceAccountPath != null && !serviceAccountPath.isEmpty();
    
    if ((!isProjectIdSet && isServiceAccountPathSet)
        || (isProjectIdSet && !isServiceAccountPathSet)) {
      throw new IllegalArgumentException(
          "Either set project id and service account or leave both fields empty");
    }
    
    if (isProjectIdSet && isServiceAccountPathSet) {
      return buildServiceClient(projectId, serviceAccountPath);
    } else {
      return buildDefaultClient();
    }
  }
  /**
   * Builds a BQ Client using default credentials
   * @return a BQ Client
   */
  private static BigQuery buildDefaultClient() {
    return BigQueryOptions.getDefaultInstance().getService();
  }
  
  /**
   * Builds a BQ Client using a ServiceAccount
   * @param projectId project id, or null if using default credentials
   * @param serviceAccountPath path to service account, or null if using default credentials
   * @return a BQ client
   * @throws FileNotFoundException if no service account found in path
   * @throws IOException if failed to read service account
   */
  private static BigQuery buildServiceClient(String projectId, String serviceAccountPath)
      throws FileNotFoundException, IOException {
      
    return BigQueryOptions.newBuilder()
          .setProjectId(projectId)
          .setCredentials(
              ServiceAccountCredentials.fromStream(new FileInputStream(
                  serviceAccountPath)))
          .build()
          .getService();
  }
}
