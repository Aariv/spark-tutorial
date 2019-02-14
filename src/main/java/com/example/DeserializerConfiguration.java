/*
 * Copyright  : 2018 (c) Zakir Hussain
 * License    : MIT
 * Maintainer : zakir@kloudone.com
 * Stability  : stable
 */

package com.example;

import com.example.dto.Cnr;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import static com.example.dto.Cnr.CnrProtoMsg.parseFrom;


@Configuration
public class DeserializerConfiguration extends AdapterConfiguration
  implements Deserializer<Cnr.CnrProtoMsg> {

  private static final Logger logger =
    LoggerFactory.getLogger(DeserializerConfiguration.class.getName());

  @Override
  public Cnr.CnrProtoMsg deserialize(String topic, byte[] data) {
    Cnr.CnrProtoMsg parseData = null;
    try {
      parseData = parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      logger.error("Message is Unparseable", e);
    }
    return parseData;
  }
}
