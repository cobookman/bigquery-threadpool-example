package com.google.BigQueryQueueDriverExamples;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryResult;
import java.util.Iterator;
import java.util.List;

public class Helpers {
  public enum Color {
    RESET("\u001B[0m"),
    RED("\u001B[31m"),
    BLACK("\u001B[30m"),
    GREEN("\u001B[32m"),
    YELLOW("\u001B[33m"),
    BLUE("\u001B[34m"),
    PURPLE("\u001B[35m"),
    CYAN("\u001B[36m"),
    WHITE("\u001B[37m");
    
    private String colorCode;
    
    private Color(String colorCode) {
      this.colorCode = colorCode;
    }
    
    public static void println(Color color, String msg) {
      System.out.println(color.colorCode + msg + Color.RESET.colorCode);
    }
  }
  
  public static void printRows(QueryResult response) {
    Iterator<List<FieldValue>> rowIterator = response.iterateAll();
    System.out.println("First 100 Rows");
    int maxRows = 100;
    while (rowIterator.hasNext() && maxRows-- > 0) {
      System.out.println(rowIterator.next());
    }
  }
}
