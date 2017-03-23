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

package com.google.bqqexamples;

import java.util.ArrayList;
import java.util.List;

/**
 * Some example queries
 */
public class ExampleQueries {
  
  public static String popularGolangPackagesSQL = "SELECT\n"
      + "  REGEXP_EXTRACT(line, r'\"([^\"])\"') AS url,\n" 
      + "  COUNT(*) AS count\n"
      + "FROM\n"  
      + "  FLATTEN( (\n"  
      + "    SELECT\n"   
      + "      SPLIT(SPLIT(REGEXP_EXTRACT( "
      + "            content, r'.*import\\s*[(]([^)]*)[)]')"
      + "            , '\\n'), ';') AS line,\n"  
      + "    FROM (\n"  
      + "      SELECT\n"  
      + "        id,\n"  
      + "        content\n"  
      + "      FROM\n"  
      + "        [bigquery-public-data:github_repos.sample_contents]\n"  
      + "      WHERE\n"  
      + "        REGEXP_MATCH(content, r'.*import\\s*[(][^)]*[)]')) AS C\n"  
      + "    JOIN (\n"  
      + "      SELECT\n"  
      + "        id\n"  
      + "      FROM\n"  
      + "        [bigquery-public-data:github_repos.sample_files]\n"  
      + "      WHERE\n"  
      + "        path LIKE '%.go'\n"  
      + "      GROUP BY\n"  
      + "        id) AS F\n"  
      + "    ON\n"  
      + "      C.id = F.id), line)\n"  
      + "GROUP BY\n"  
      + "  url\n"  
      + "HAVING\n"  
      + "  url IS NOT NULL\n"  
      + "ORDER BY\n"  
      + "  count DESC\n"  
      + "LIMIT 10\n";
  
  public static String popularJavaPackagesSQL = "SELECT\n"  
      + "  package,\n"  
      + "  COUNT(*) count\n"  
      + "FROM (\n"  
      + "  SELECT\n"  
      + "    REGEXP_EXTRACT(line, r' ([a-z0-9\\._]*)\\.') package,\n"  
      + "    id\n"  
      + "  FROM (\n"  
      + "    SELECT\n"  
      + "      SPLIT(content, '\\n') line,\n"  
      + "      id\n"  
      + "    FROM\n"  
      + "      [bigquery-public-data:github_repos.sample_contents]\n"  
      + "    WHERE\n"  
      + "      content CONTAINS 'import'\n"  
      + "      AND sample_path LIKE '%.java'\n"  
      + "    HAVING\n"  
      + "      LEFT(line, 6)='import' )\n"  
      + "  GROUP BY\n"  
      + "    package,\n"  
      + "    id )\n"  
      + "GROUP BY\n"  
      + "  1\n"  
      + "ORDER BY\n"  
      + "  count DESC\n"  
      + "LIMIT\n"  
      + "  40;\n";
 
  public static String thisShouldNeverHappenSQL = "SELECT\n"  
      + "  SUM(copies)\n"  
      + "FROM\n"  
      + "  [bigquery-public-data:github_repos.sample_contents]\n"  
      + "WHERE\n"  
      + "  NOT binary\n"  
      + "  AND content CONTAINS 'This should never happen'\n";
  
  public static String wikipediaTopPageSQL = "SELECT title, SUM(views) views\n"  
      + "FROM\n"  
      + "  [bigquery-samples:wikimedia_pageviews.201112],\n"  
      + "  [bigquery-samples:wikimedia_pageviews.201111]\n"  
      + "WHERE wikimedia_project = \"wp\"\n"  
      + "AND REGEXP_MATCH(title, 'Red.*t')\n"  
      + "GROUP BY title\n"  
      + "ORDER BY views DESC\n";
  
  public static String uniqueShakespearWordSQL = "SELECT COUNT(UNIQUE(word)) \n" 
      + "FROM [bigquery-public-data:samples.shakespeare]";
  
  public static List<String> queries() {
    ArrayList<String> qs = new ArrayList<String>();
    qs.add(popularGolangPackagesSQL);
    qs.add(popularJavaPackagesSQL);
    qs.add(thisShouldNeverHappenSQL);
    qs.add(wikipediaTopPageSQL);
    qs.add(uniqueShakespearWordSQL);
    return qs;
  }
}
