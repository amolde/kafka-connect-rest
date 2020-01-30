package com.tm.kafka.connect.rest.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class InstanceOfValidatorTest {

  InstanceOfValidator validator = new InstanceOfValidator(TestClass.class);

  @Test
  public void ensureValidTest_sameClass() {
    validator.ensureValid("test", TestClass.class);
  }

  @Test
  public void ensureValidTest_subclass() {
    validator.ensureValid("test", TestSubClass.class);
  }

  @Test(expected = ConfigException.class)
  public void ensureValidTest_wrongClass() {
    validator.ensureValid("test", Object.class);
  }

  @Test(expected = ConfigException.class)
  public void ensureValidTest_notAClass() {
    validator.ensureValid("test", new Object());
  }

  @Test
  public void toString1() {
  }


  private static class TestClass {
  }

  private static class TestSubClass extends TestClass {
  }
}
