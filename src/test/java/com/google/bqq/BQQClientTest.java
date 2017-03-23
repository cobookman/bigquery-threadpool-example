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

import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests BQQClient class.
 */
public class BQQClientTest {
  private static final String EXAMPLE_QUERY_SQL = "SELECT "
      + "COUNT(UNIQUE(word)) "
      + "FROM [bigquery-public-data:samples.shakespeare]";
  
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
  public void testStartup_BadServiceAccountPath()
      throws FileNotFoundException, IOException {
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
      c.queueQuery(EXAMPLE_QUERY_SQL, true);
    }
    
    Thread.sleep(5);
    System.out.println("Query count: " + c.getNumJobs());
    Assert.assertEquals(Integer.valueOf(99), c.getNumJobs());
    c.shutdown();
  }
  
  @Test
  public void testQueueQuery_QueuesQuery()
      throws FileNotFoundException, IOException,
      InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    Future<QueryResult> f1 = c.queueQuery(QueryRequest.newBuilder(EXAMPLE_QUERY_SQL)
        .setUseLegacySql(true).build());
    Future<QueryResult> f2 = c.queueQuery(EXAMPLE_QUERY_SQL, true);
    
    
    QueryResult r1 = BQQClient.getQueryResult(f1);
    QueryResult r2 = BQQClient.getQueryResult(f2);
    
    Assert.assertEquals(r1.getTotalRows(), r2.getTotalRows());    
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_LegacySql()
      throws FileNotFoundException, IOException,
      InterruptedException, ExecutionException, BQQException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query. mExampleQuery is legacy sql, 
    Future<QueryResult> f = c.queueQuery(QueryRequest.newBuilder(EXAMPLE_QUERY_SQL)
        .setUseLegacySql(false).build());
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_NoSQLQuery()
      throws FileNotFoundException, IOException,
      InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query.
    Future<QueryResult> f = c.queueQuery("", false);
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testGetQueryResult_PassesBQErrors_MissingParameters()
      throws FileNotFoundException, IOException,
      InterruptedException, BQQException, ExecutionException {
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
  public void testGetQueryResult_PassesBQErrors_ProjectNotFound()
      throws FileNotFoundException, IOException,
      InterruptedException, BQQException, ExecutionException {
    BQQClient c = new BQQClient();
    c.startup(1);
    
    // this should be a failing query, parameters not all set, missing corpus.
    Future<QueryResult> f = c.queueQuery(
        "SELECT count(*) FROM [some-bs-project:some.bstable]", true);
    
    thrown.expect(BQQException.class);
    BQQClient.getQueryResult(f);
  }
  
  @Test
  public void testShutdown_CanTeardownThreadPool() throws Exception {
    BQQClient c = new BQQClient();
    c.startup(3);
    
    // Schedule some tasks
    for (int i = 0; i < 10; ++i) {
      c.queueQuery(EXAMPLE_QUERY_SQL, true);
    }
    Thread.sleep(10);
    c.shutdown();
  }
}
