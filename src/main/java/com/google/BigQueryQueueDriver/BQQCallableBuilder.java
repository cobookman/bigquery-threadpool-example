package com.google.BigQueryQueueDriver;

import com.google.cloud.bigquery.QueryParameterValue;

import java.util.Map;

public class BQQCallableBuilder {
  private String mProjectId;
  private String mQuery;
  private String mServiceAccountPath = "";
  private Boolean mUseLegacySQL = false;
  private Map<String, QueryParameterValue>
    
  public BQQCallableBuilder() {
  }

  public BQQCallableBuilder setProjectId(String projectId) {
    mProjectId = projectId;
    return this;
  }
  
  public BQQCallableBuilder setQuery(String query) {
    mQuery = query;
    return this;
  }
  
  public BQQCallableBuilder setParameters(Map<String, QueryParameterValue> queryParams) {
    mQueryParams = queryParams;
  }
  
  public BQQCallableBuilder setServiceAccountPath(String serviceAccountPath) {
    mServiceAccountPath = serviceAccountPath;
    return this;
  }
  
  public BQQCallableBuilder setUseLegacySQL(boolean useLegacySQL) {
    mUseLegacySQL = useLegacySQL;
    return this;
  }
  
  public BQQCallable build() {
    if (mQuery.isEmpty()) {
      throw new IllegalArgumentException("Need to specify a SQL Query");
    }
    
    if (!mServiceAccountPath.isEmpty() && mProjectId.isEmpty()) {
      throw new IllegalArgumentException("Need a project ID Specified if using service account");
    }
    
    return new BQQCallable(mProjectId, mQuery, mQueryParams,
        mServiceAccountPath, mUseLegacySQL);
  }
}
