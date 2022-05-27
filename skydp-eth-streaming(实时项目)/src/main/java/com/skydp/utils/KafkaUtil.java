package com.skydp.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Objects;

/**
 * 获取kafkaSource
 */
public class KafkaUtil {
    /**
     *
     * @param topic:kafka主题
     * @param groupId：消费组组
     * @return KafkaSource
     */
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        //Set the parameters for connecting kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigUtil.getStringConfig("BOOTSTRAP_SERVERS"))
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "524288000")
                .setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "524288000")
                .build();
        return source;
    }

    /**
     *
     * @param topic:消费组组
     * @return:返回KafkaSink
     * DeliverGuarantee:EXACTLY_ONCE
     */
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(ConfigUtil.getStringConfig("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
        return sink;
    }
}
