BigQuery ThreadPoolExecutor Example Code
==========================

[![Build
Status](https://api.travis-ci.org/cobookman/bigquery-threadpool-example.svg?branch=master)](https://travis-ci.org/cobookman/bigquery-threadpool-example)

Example code on using a ThreadPoolExecutor to have a First in First Out Queue of Queries
which are handled by N worker threads.

This is not an official Google product.

Quickstart
----------

1) Clone this repo and copy the all files in `src/main/com/google/bqq/` to your project source directory,
   under the same path.

2) Add the Google [Cloud BigQuery Java SDK](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-bigquery) 
   to your Maven or gradle build. You can find instructions on how to do this [here](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-bigquery#quickstart).

3) If you'd like to use a service account, make sure to create one and download it and keep tabs on the path its stored in.


Example Applications
--------------------

* [Async Query Execution](https://github.com/cobookman/bigquery-threadpool-example/blob/master/src/main/java/com/google/bqqexamples/ExampleAsync.java):
  Queues up many queries to be executed, and then blocks until all queries are done. It also checks on the status of queries while they run to handle results. This is also possible using a [CompletionService](http://stackoverflow.com/questions/19348248/waiting-on-a-list-of-future)

* [Sync Query Execution](https://github.com/cobookman/bigquery-threadpool-example/blob/master/src/main/java/com/google/bqqexamples/ExampleSync.java):
  Queues up a query then blocks til completion.

* [Stress Test](https://github.com/cobookman/bigquery-threadpool-example/blob/master/src/main/java/com/google/bqqexamples/ExampleStressTest.java):
  Queues up many queries, and many worker threads to stress test this library. In this example there's 100 BQ Queries and 40 concurrent workers.


# How to Use

## Creating a Client:

### Using a service account

Both a project Id and absolute path to a service account need to be provided
```
BQQClient c = new BQQClient("my-awesome-project", "/home/bookman/service_account.json");
```

### Using a default credentials

Here is some information on (Application Default Credentials)[https://developers.google.com/identity/protocols/application-default-credentials].

Basically when running this library on a Computer that has the gcloud utility installed, you do not need to specify a
service account to run against the same project Id that the gcloud utility is running against.

To setup the client simply run:

```
BQQClient c = new BQQClient();
```

## Starting up the client

The BQQClient uses a [ThreadPoolExecutor](http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html)
underneath to queue up BigQuery Requests. Before any jobs can be run the client needs to be started up.

This will also do a sanity check to make sure that either both a project Id & Service account are set, or that neither are set and
default credentials will be used instead.

Here is an example of us Starting up a Client with a maximum of 40 concurrent queries.

```
BQQClient c = new BQQClient();
try {
    c.startup(numberOfConcurrentQueries);
} catch (IOException e) {
    // handle issue with loading credentials
    e.printStackTrace();
}
```

## Building a QueryRequest

The BigQuery SDK has a class called [QueryRequest](http://googlecloudplatform.github.io/google-cloud-java/0.10.0/apidocs/com/google/cloud/bigquery/QueryRequest.html).

These QueryRequest objects can be build using the QueryRequest factory method as shown below:

```
QueryRequest queryRequest = QueryRequest.newBuilder("SELECT * FROM [bigquery-public-data:samples.shakespeare]")
    .setUseLegacySql(true)
    .setUseQueryCache(true)
    .build()
```


You can use this class to build [Parameterized Queries](https://cloud.google.com/bigquery/querying-data#running_parameterized_queries),
which prevent SQL injections:

```
int minWordCount = 10;
String corpus = "tempest";
String parameterizedSql = "SELECT word, word_count "
    + "FROM `bigquery-public-data.samples.shakespeare` "
    + "WHERE corpus = @corpus "
    + "AND word_count >= @min_word_count "
    + "ORDER BY word_count DESC";
    
QueryRequest queryRequest = QueryRequest.newBuilder(parameterizedSql)
    .addNamedParameter("corpus", QueryParameterValue.string(corpus))
    .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
    .setUseLegacySql(false)
    .build();
```

## Queuing a Query

All Queries that are queued return a [Future](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Future.html). This allows you to do work while you wait for the response.

### Using a QueryRequest

Here's how to queue up a QueryRequest. The Returned
```
QueryRequest queryRequest = QueryRequest.newBuilder("SELECT * FROM [bigquery-public-data:samples.shakespeare]")
    .setUseLegacySql(true)
    .build()

Future<QueryResult> queryResultFuture = c.queue(queryRequest);
```

### Using a SQL Statement String
 
If you just want to queue up a SQL statement and you are ok with the default QueryRequest options,
then you can...

```java
Future<QueryResult> queryResultFuture = c.queue("SELECT * FROM [bigquery-public-data:samples.shakespeare]", true);
```


## Resolving a Future<QueryResult>

You can pass the `Future<QueryResult>` to the `BQQClient.getQueryResult` method.
This method deals with cleaning up exceptions thrown by the worker threads.
It will also block until a response is ready.

```java
Future<QueryResult> queryResultFuture = c.queue("SELECT * FROM [bigquery-public-data:samples.shakespeare]", true);
QueryResult queryResult = BQQClient.getQueryResult(queryResultFuture);
```

## Shutting down Client

Before shutting down the client you'll want to wait till all running jobs are completed.
This can be done by using the `BQQClient.shutdown` method.

You can specify a time to wait till forcing the threads to be killed by JVM

```java
c.shutdown(3000); // give threads 3000ms (3 second) to finish their task before forcing a shutdown
```

Or you can just use the default timeout time of 1000ms

```java
c.shutdown();
```

## Handling Exceptions

Let's say you ended up running into a BigQuery Error. Maybe due to an invalid query, auth issue, or exceeding 
2000 slots. These errors are all captured by this client and easily handled.

You can find a [list of error codes in our docs](https://cloud.google.com/bigquery/troubleshooting-errors).
In the `BigQueryError` class the method `getReason()` will
return an Error Code such-as `accessDenied` or `quotaExceeded`.

In this case there is a missing parameter in the query.

```java
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

    // Missing the @corpus parameter
    .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
    .setUseLegacySql(false)
    .build();

Future<QueryResult> f = c.queueQuery(parameterizedQueryRequest);
    
try {
    BQQClient.getQueryResult(f);
} catch (BQQException e) {
    for (BigQueryError be : errs.getBQErrors()) {
        if (be.getReason().contains("invalidQuery")) {
            ...
        } else if (be.getReason().contains("quotaExceeded")) {
            ...
        } else if (be.getReason().contains("rateLimitExceeded")) {
            ...
        } else if (be.getReason().contains("resourcesExceeded")) {
            ...
        }
    }
}
```

## A Complete Example

Here's this all put together in a complete example:

```java
BQQClient c = new BQQClient();
c.startup(10);

int minWordCount = 10;
String corpus = "tempest";
String parameterizedSql = "SELECT word, word_count "
    + "FROM `bigquery-public-data.samples.shakespeare` "
    + "WHERE corpus = @corpus "
    + "AND word_count >= @min_word_count "
    + "ORDER BY word_count DESC";
    
QueryRequest queryRequest1 = QueryRequest.newBuilder(parameterizedSql)
    .addNamedParameter("corpus", QueryParameterValue.string(corpus))
    .addNamedParameter("min_word_count", QueryParameterValue.int64(minWordCount))
    .setUseLegacySql(false)
    .build();

Future<QueryResult> qrf1 = qc.queue(queryRequest);
Future<QueryResult> qrf2 = c.queue("SELECT * FROM [bigquery-public-data:samples.shakespeare]", true);

/**
 * do some work, such-as render some HTML
 */

QueryResult queryResult1 = BQQClient.getQueryResult(qrf1);
QueryResult queryResult2 = BQQClient.getQueryResult(qrf2);

// Print out results
Iterator<List<FieldValue>> it = queryResult1.iterateAll();
System.out.println("First 100 Rows");
int maxRows = 100;
while (it.hasNext() && maxRows-- > 0) {
    List<FieldValue> row = iter.next();
    System.out.println(row);
}

/**
 * Do more work
  */

c.shutdown();
```
