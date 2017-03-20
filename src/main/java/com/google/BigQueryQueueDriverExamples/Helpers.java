package com.google.BigQueryQueueDriverExamples;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryResult;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

public class Helpers {
    
  public static void printRows(QueryResult response) {
    Iterator<List<FieldValue>> rowIterator = response.iterateAll();
    System.out.println("First 100 Rows");
    int maxRows = 100;
    while (rowIterator.hasNext() && maxRows-- > 0) {
      System.out.println(rowIterator.next());
    }
  }
}
