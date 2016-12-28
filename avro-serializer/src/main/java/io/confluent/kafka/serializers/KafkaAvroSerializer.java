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
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.Map;

public class KafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {

  private boolean isKey;

  // TODO: should be conf driven
  private static final String SERIALIZER_CONFIG = "file:serializer_config.json";
  private static final String topicSubkey = getTopicSubkey();

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
      URL resource = new URL(SERIALIZER_CONFIG);

      String content = IOUtils.toString(resource.openStream()); // TODO: deprecated call
      return new JSONObject(content).getString("key_column");
    } catch (NoSuchFileException | FileNotFoundException e) {
      System.err.println("************ Could not find config " + SERIALIZER_CONFIG + "                            ************");
      System.err.println("************ All schemas will be registered with the same endpoint in the schema registry. ************");
      System.err.println("************ If you are applying schemas to embedded JSON things will break.               ************");
      System.err.println("************ If you are not you probably don't care.                                       ************");
    } catch (IOException | NullPointerException e) {
      throw new RuntimeException(e.toString());
    }

    return null;
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
      if (topicSubkey == null || !(record instanceof GenericRecord)) {
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
