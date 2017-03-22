package com.google.bqq;

import com.google.cloud.bigquery.QueryRequest;

/**
 * Builder for BQQCallables
 */
public class BQQCallableBuilder {
  private String mProjectId;
  private String mServiceAccountPath = "";
  private QueryRequest mQueryRequest;
    
  public BQQCallableBuilder() {}

  public BQQCallableBuilder setProjectId(String projectId) {
    mProjectId = projectId;
    return this;
  }
  
  public BQQCallableBuilder setQueryRequest(QueryRequest queryRequest) {
    mQueryRequest = queryRequest;
    return this;
  }
  
  
  public BQQCallableBuilder setServiceAccountPath(String serviceAccountPath) {
    mServiceAccountPath = serviceAccountPath;
    return this;
  }
  
  public BQQCallable build() {
    if (mQueryRequest == null) {
      throw new IllegalArgumentException("Need to specify a queryRequest");
    }

    boolean usingServiceAccount = mServiceAccountPath != null && !mServiceAccountPath.isEmpty();
    if (usingServiceAccount && mProjectId.isEmpty()) {
      throw new IllegalArgumentException("Need a project ID Specified if using service account");
    }
    
    return new BQQCallable(mProjectId, mServiceAccountPath, mQueryRequest);
  }
}
