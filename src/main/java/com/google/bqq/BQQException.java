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
