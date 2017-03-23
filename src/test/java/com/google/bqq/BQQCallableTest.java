package com.google.bqq;

import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests on BQQCallable Class.
 */
public class BQQCallableTest {
  private static final String EXAMPLE_QUERY_SQL = "SELECT "
      + "COUNT(UNIQUE(word)) "
      + "FROM [bigquery-public-data:samples.shakespeare]";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCall_SuccessfullyRuns()
      throws FileNotFoundException, BQQException, InterruptedException, IOException {
    BQQCallable c = new BQQCallableBuilder()
      .setQueryRequest(QueryRequest
          .newBuilder(EXAMPLE_QUERY_SQL)
          .setUseLegacySql(true)
          .build())
      .build();
    QueryResult r = c.call();
    
    Assert.assertTrue("There are rows returned from query", r.getTotalRows() > 0);
  }
  
  
  @Test
  public void testCall_ThrowsException_EmptyQuery()
      throws FileNotFoundException, BQQException, InterruptedException, IOException {
    BQQCallable c = new BQQCallableBuilder()
        .setQueryRequest(QueryRequest
            .newBuilder("")
            .setUseLegacySql(true)
            .build())
        .build();
    
    thrown.expect(BQQException.class);
    c.call();
  }
  
  @Test
  public void testCall_ThrowsException_BadQuery()
      throws FileNotFoundException, BQQException, InterruptedException, IOException {
    BQQCallable c = new BQQCallableBuilder()
        .setQueryRequest(QueryRequest
            .newBuilder("SELECT count(*) FROM [some-bs-project:some.bstable]")
            .setUseLegacySql(true)
            .build())
        .build();
    
    thrown.expect(BQQException.class);
    c.call();
  }
  
  @Test
  public void testCall_ThrowsException_MissingParams()
      throws FileNotFoundException, BQQException, InterruptedException, IOException {
    int minWordCount = 10;
    String parameterizedSql = "SELECT word, word_count\n"
        + "FROM `bigquery-public-data.samples.shakespeare`\n"
        + "WHERE corpus = @corpus\n"
        + "AND word_count >= @min_word_count\n"
        + "ORDER BY word_count DESC";
    
    QueryRequest queryRequest = QueryRequest
        .newBuilder(parameterizedSql)
        .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
        .setUseLegacySql(false)
        .build();

    BQQCallable c = new BQQCallableBuilder()
        .setQueryRequest(queryRequest)
        .build();
    
    thrown.expect(BQQException.class);
    c.call();
  }
}
