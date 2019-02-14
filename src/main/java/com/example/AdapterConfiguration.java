package com.example;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class AdapterConfiguration {

  public void close() {}

  public void configure(Map<String, ?> configuration, boolean isKey) {}
}