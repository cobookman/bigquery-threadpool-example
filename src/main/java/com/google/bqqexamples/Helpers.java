package com.google.bqqexamples;

import com.google.bqq.BQQException;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryResult;
import java.util.Iterator;
import java.util.List;

/**
 * Helper functions.
 */
public class Helpers {
  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_RED = "\u001B[31m";

  /**
   * Print a query result's first 100 rows.
   * @param response a bigquery response
   */
  public static void printRows(QueryResult response) {
    Iterator<List<FieldValue>> iter = response.iterateAll();
    System.out.println("First 100 Rows");
    int maxRows = 100;
    while (iter.hasNext() && maxRows-- > 0) {
      List<FieldValue> row = iter.next();
      System.out.println(row);
    }
  }
  
  /**
   * Prints out all the BigQueryErrors in red.
   * @param errs
   */
  public static void printErrorCodes(BQQException errs) {
    for (BigQueryError be : errs.getBQErrors()) {
      System.out.println(ANSI_RED + "\t\tError Code: " + be.getReason() + ANSI_RESET);
      System.out.println(ANSI_RED + "\t\t"
          + "https://cloud.google.com/bigquery/troubleshooting-errors" 
          + ANSI_RESET);
    }
  }
}
