# Debezium Connect 2.5 + the Confluent Avro converter.
#
# The stock debezium/connect image ships Apicurio converters, not Confluent's,
# so `io.confluent.connect.avro.AvroConverter` is missing and the worker fails
# to start when configured against Confluent Schema Registry. We copy the
# self-contained kafka-serde-tools bundle (converter + serializer + registry
# client + transitive deps) onto Debezium's plugin path.
FROM confluentinc/cp-kafka-connect:7.5.0 AS confluent

FROM debezium/connect:2.5
COPY --from=confluent /usr/share/java/kafka-serde-tools/ /kafka/connect/confluent-avro/
