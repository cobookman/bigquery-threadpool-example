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

import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests BQQService Class.
 */
public class BQQServiceTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuildBQClient_usingdefault()
      throws FileNotFoundException, IOException {
    BQQServiceFactory.buildClient(null, null);
  }
  
  @Test
  public void testBuildBQClient_ThrowsError_NoServiceAccount()
      throws FileNotFoundException, IOException {
    thrown.expect(FileNotFoundException.class);
    BQQServiceFactory.buildClient("some-project", "some-non-exsistent-file.json");
  }
  
  @Test
  public void testBuildBQClient_ThrowsError_NoProjectID()
      throws FileNotFoundException, IOException {
    thrown.expect(IllegalArgumentException.class);
    BQQServiceFactory.buildClient("some-project", "");
  }
}
