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
