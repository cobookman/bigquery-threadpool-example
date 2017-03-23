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

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic class to handle all Exceptions related to BigQuery execution.
 * Is a simple wrapper class to handle all BigQuery thrown errors.
 */
public class BQQException extends Exception {
  private List<BigQueryError> mBQErrors;
  
  public BQQException() {
    super();
  }
  
  public BQQException(String message) {
    super(message);
  }
    
  public BQQException(String message, BigQueryError bqError) {
    super(message + "\n" + bqError.toString());
    mBQErrors = new ArrayList<>();
    mBQErrors.add(bqError);
  }
  
  public BQQException(BigQueryError bqError) {
    super(bqError.getMessage());
    mBQErrors = new ArrayList<>();
    mBQErrors.add(bqError);
  }
  
  public BQQException(String message, BigQueryException bqError) {
    super(message, bqError);
    mBQErrors = new ArrayList<>();
    mBQErrors.add(bqError.getError());
  }

  public BQQException(BigQueryException bqError) {
    super(bqError);
    mBQErrors = new ArrayList<>();
    mBQErrors.add(bqError.getError());
  }

  public BQQException(String message, List<BigQueryError> bqErrors) {
    super(message + "\n" + bqErrors.toString());
    mBQErrors = bqErrors;
  }

  public List<BigQueryError> getBQErrors() {
    return mBQErrors;
  }
}
