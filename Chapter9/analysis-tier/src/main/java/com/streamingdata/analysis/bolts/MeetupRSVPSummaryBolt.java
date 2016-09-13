package com.streamingdata.analysis.bolts;

import com.clearspring.analytics.stream.StreamSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class MeetupRSVPSummaryBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(MeetupRSVPSummaryBolt.class);
    private static final long serialVersionUID = 6749134782408285639L;
    private static StreamSummary<String> streamSummary = new StreamSummary<>(100000);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private OutputCollector collector;


    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        //this is the rsvp info. Need to extact it to a JSON object and grab the groups
        //the structure looks like this:
        /**
          {
             "venue": {
                .....
             },

             "member": {
                .....
             },

             "event": {
                ....
             },
             "group": {
             "group_topics": [
                {
                 "urlkey": "social",
                 "topic_name": "Social"
                },
             ....
         */
        String jsonString = tuple.getString(4);
        try {
            JsonNode root = objectMapper.readTree(jsonString);
            JsonNode groupTopics = root.get("group").get("group_topics");
            Iterator<JsonNode> groupTopicsItr = groupTopics.iterator();
            while(groupTopicsItr.hasNext()){
                JsonNode groupTopic = groupTopicsItr.next();
                final String topicName = groupTopic.get("topic_name").asText();
                streamSummary.offer(topicName);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
        }

        try {
            collector.emit(new Values(UUID.randomUUID().toString(),objectMapper.writeValueAsString(streamSummary.topK(10))));
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage(),e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("objid","summary"));
    }

}