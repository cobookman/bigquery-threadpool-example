package com.google.bqq;

import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Assert;

public class BQQClientTest {
  private static String mExampleQuery = "SELECT COUNT(UNIQUE(word)) FROM [bigquery-public-data:samples.shakespeare]";
  
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConstructor_EmptyStringArguments() {
    thrown.expect(IllegalArgumentException.class);
    new BQQClient("", "");
  }
  
  @Test
  public void testConstructor_NullArguments() {
    thrown.expect(IllegalArgumentException.class);
    new BQQClient(null, null);
  }
  
  @Test
  public void testStartup_BadServiceAccountPath() throws FileNotFoundException, IOException {
    BQQClient c = new BQQClient("some_bs_project", "some_bs_key.json");
    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage("some_bs_key.json (No such file or directory)");
    c.startup(10);
  }
  
  @Test
  public void testGetNumJobs_GivesAccurateCount() throws Exception {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    for (int i = 0; i < 100; ++i) {
      c.queueQuery(mExampleQuery, true);
    }
    
    Thread.sleep(5);
    System.out.println("Query count: " + c.getNumJobs());
    Assert.assertEquals(Integer.valueOf(99), c.getNumJobs());
    c.shutdown();
  }
  
  @Test
  public void testQueueQuery_QueuesQuery() throws FileNotFoundException, IOException, InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    Future<QueryResult> f1 = c.queueQuery(QueryRequest.newBuilder(mExampleQuery)
        .setUseLegacySql(true).build());
    Future<QueryResult> f2 = c.queueQuery(mExampleQuery, true);
    
    
    QueryResult r1 = BQQClient.getQueryResult(f1);
    QueryResult r2 = BQQClient.getQueryResult(f2);
    
    Assert.assertEquals(r1.getTotalRows(), r2.getTotalRows());    
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_LegacySql() throws FileNotFoundException, IOException, InterruptedException, ExecutionException, BQQException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query. mExampleQuery is legacy sql, 
    Future<QueryResult> f = c.queueQuery(QueryRequest.newBuilder(mExampleQuery)
        .setUseLegacySql(false).build());
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_NoSQLQuery() throws FileNotFoundException, IOException, InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query.
    Future<QueryResult> f = c.queueQuery("", false);
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_MissingParameters() throws FileNotFoundException, IOException, InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query, parameters not all set, missing corpus.
    int minWordCount = 10;
    String parameterizedSql = "SELECT word, word_count\n"
        + "FROM `bigquery-public-data.samples.shakespeare`\n"
        + "WHERE corpus = @corpus\n"
        + "AND word_count >= @min_word_count\n"
        + "ORDER BY word_count DESC";
    
    QueryRequest parameterizedQueryRequest = QueryRequest
        .newBuilder(parameterizedSql)
        .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
        .setUseLegacySql(false)
        .build();
    Future<QueryResult> f = c.queueQuery(parameterizedQueryRequest);
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_ProjectNotFound() throws FileNotFoundException, IOException, InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query, parameters not all set, missing corpus.
    Future<QueryResult> f = c.queueQuery("SELECT count(*) FROM [some-bs-project:some.bstable]", true);
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testShutdown_CanTeardownThreadPool() throws Exception {
    BQQClient c = new BQQClient();
    c.startup(3);
    
    // Schedule some tasks
    for (int i = 0; i < 10; ++i) {
      c.queueQuery(mExampleQuery, true);
    }
    Thread.sleep(10);
    c.shutdown();
  }
}
