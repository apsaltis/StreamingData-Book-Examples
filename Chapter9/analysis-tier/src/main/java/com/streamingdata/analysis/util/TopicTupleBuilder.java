package com.streamingdata.analysis.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.tuple.Values;

import java.util.List;

public class TopicTupleBuilder<K,V> extends KafkaSpoutTupleBuilder<K,V> {
    private static final long serialVersionUID = 3756126573829086698L;

    /**
     * @param topics list of topics that use this implementation to build tuples
     */
    public TopicTupleBuilder(String... topics) {
        super(topics);
    }

    @Override
    public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
        return new Values(consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                consumerRecord.value());
    }
}
