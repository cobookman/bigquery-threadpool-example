package com.google.bqq;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests on BQQCallableBuilder class
 */
public class BQQCallableBuilderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuild_ForceQuery() {
    
    thrown.expect(IllegalArgumentException.class);
    new BQQCallableBuilder()
      .setProjectId("some-project")
      .setServiceAccountPath("some-service-account")
      .build();
  }
  
  @Test
  public void testBuild_ForceProjectIfServiceAcct() {

    thrown.expect(IllegalArgumentException.class);
    new BQQCallableBuilder()
      .setServiceAccountPath("some-service-account")
      .build();
  }
  
  @Test
  public void testBuild_ForceServiceAcctIfProjectQuery() {

    thrown.expect(IllegalArgumentException.class);
    new BQQCallableBuilder()
      .setProjectId("some-project")
      .build();
  }
  
  
}
