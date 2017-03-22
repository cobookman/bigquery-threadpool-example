package com.google.bqq;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BQQServiceTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuildBQClient_usingdefault() throws FileNotFoundException, IOException {
    BQQService.buildClient(null, null);
  }
  
  @Test
  public void testBuildBQClient_ThrowsError_NoServiceAccount() throws FileNotFoundException, IOException {
    thrown.expect(FileNotFoundException.class);
    BQQService.buildClient("some-project", "some-non-exsistent-file.json");
  }
  
  @Test
  public void testBuildBQClient_ThrowsError_NoProjectID() throws FileNotFoundException, IOException {
    thrown.expect(IllegalArgumentException.class);
    BQQService.buildClient("some-project", "");
  }
}