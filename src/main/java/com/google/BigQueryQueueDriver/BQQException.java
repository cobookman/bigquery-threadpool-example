package com.google.BigQueryQueueDriver;

import com.google.cloud.bigquery.BigQueryError;
import java.util.List;

public class BQQException extends Exception {
  private List<BigQueryError> mBQErrors;
  
  public BQQException() {
    super();
  }
  
  public BQQException(String message) {
    super(message);
  }
  
  public BQQException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public BQQException(Throwable cause) {
    super(cause);
  }
  
  public BQQException(String message, List<BigQueryError> bqErrors) {
    super(message);
    mBQErrors = bqErrors;
  }
  
  public List<BigQueryError> getBQErrors() {
    return mBQErrors;
  }
}
