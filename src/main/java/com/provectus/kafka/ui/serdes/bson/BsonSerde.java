package com.provectus.kafka.ui.serdes.bson;

import java.util.Collections;
import java.util.Optional;

import com.provectus.kafka.ui.serde.api.DeserializeResult;
import com.provectus.kafka.ui.serde.api.PropertyResolver;
import com.provectus.kafka.ui.serde.api.RecordHeaders;
import com.provectus.kafka.ui.serde.api.SchemaDescription;
import com.provectus.kafka.ui.serde.api.Serde;

import org.bson.Document;

public class BsonSerde implements Serde {

  @Override
  public void configure(PropertyResolver serdeProperties,
                        PropertyResolver clusterProperties,
                        PropertyResolver appProperties) {
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.empty();
  }

  @Override
  public Optional<SchemaDescription> getSchema(String topic, Target target) {
    return Optional.empty();
  }

  @Override
  public boolean canDeserialize(String topic, Target target) {
    return true;
  }

  @Override
  public boolean canSerialize(String topic, Target target) {
    return true;
  }

  @Override
  public Serializer serializer(String topic, Target target) {
    return new Serializer() {
      @Override
      public byte[] serialize(String inputString) {
        try {
          Document document = Document.parse(inputString);
          return BsonToBinary.toBytes(document);
        } catch (Exception e) {
          throw new RuntimeException("Serialization error", e);
        }
      }
    };
  }

  @Override
  public Deserializer deserializer(String topic, Target target) {
    return new Deserializer() {
      @Override
      public DeserializeResult deserialize(RecordHeaders recordHeaders, byte[] bytes) {
        try {
          return new DeserializeResult(
              BsonToBinary.toDocument(bytes).toJson(),
              DeserializeResult.Type.JSON,
              Collections.emptyMap());
        } catch (Exception e) {
          throw new RuntimeException("Deserialization error", e);
        }
      }
    };
  }
}
