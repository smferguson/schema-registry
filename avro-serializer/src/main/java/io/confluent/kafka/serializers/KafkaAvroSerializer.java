/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class KafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {

  private boolean isKey;

  private static String topicSubkey = getTopicSubkey();

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaAvroSerializer() {

  }

  public KafkaAvroSerializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(serializerConfig(props));
  }

  private static String getTopicSubkey() {
    try {
      String content = new String(Files.readAllBytes(Paths.get("config.json")));
      JSONObject obj = new JSONObject(content);
      return obj.get("key_column").toString();
    } catch (IOException | NullPointerException e) {
      // TODO: log this
      return null;
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaAvroSerializerConfig(configs));
  }

  @Override
  public byte[] serialize(String topic, Object record) {
    return serializeImpl(getSubjectName(topic, isKey, record), record);
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected static String getSubjectName(String topic, boolean isKey, Object record) {
    if (isKey) {
      return topic + "-key";
    } else {
      if (topicSubkey == null) {
        return topic + "-value";
      } else {
        return topic + "-" + ((GenericRecord) record).get(topicSubkey) + "-value";
      }
    }
  }



  @Override
  public void close() {

  }
}
