/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamingdata.analysis;

import com.streamingdata.analysis.bolts.KafkaBolt;
import com.streamingdata.analysis.bolts.MeetupRSVPSummaryBolt;
import com.streamingdata.analysis.util.TopicTupleBuilder;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class TopMeetupTopicsTopology {

    private static final Logger LOG = Logger.getLogger(TopMeetupTopicsTopology.class);
    private static final String TOPOLOGY_NAME = "TopMeetupTopics";
    private static final String TOPIC_NAME = "meetup-raw-rsvps";
    private static final String STREAM_NAME = "meetup-rsvp-stream";

    private TopMeetupTopicsTopology() {

    }

    private void runLocally() throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(this.TOPOLOGY_NAME, getConfig(), buildTopolgy());
        stopWaitingForInput();
    }


    private void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        StormSubmitter.submitTopology(TOPOLOGY_NAME, getConfig(), buildTopolgy());
    }

    private Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    private StormTopology buildTopolgy() {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig()), 1);
        topologyBuilder.setBolt("rsvpSummarizer", new MeetupRSVPSummaryBolt(), 1).globalGrouping("kafka_spout", STREAM_NAME);
        topologyBuilder.setBolt("summarySerializer", new KafkaBolt(), 1).shuffleGrouping("rsvpSummarizer");
        return topologyBuilder.createTopology();
    }

    private KafkaSpoutConfig<String, String> getKafkaSpoutConfig() {
        return new KafkaSpoutConfig.Builder<>(getKafkaConsumerProps(), getKafkaSpoutStreams(), getTuplesBuilder(), getRetryService())
                .build();
    }

    private KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
        return new KafkaSpoutStreamsNamedTopics.Builder(outputFields, STREAM_NAME, new String[]{TOPIC_NAME})
                .build();
    }

    private Map<String, Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "true");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, TOPIC_NAME + "-group");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    private KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {

        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<>(
                new TopicTupleBuilder(TOPIC_NAME)
        ).build();
    }
    private KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }







    private void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP THE TOPOLOGY");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Submits (runs) the topology.
     *
     * Usage: "TopMeetupTopicsTopology [topology-name] [local|remote]"
     *
     * By default, the topology is run locally under the name "TopMeetupTopics".
     *
     * Examples:
     *
     * ```
     * # Runs in local mode (LocalCluster)
     * $ storm jar storm-starter-jar-with-dependencies.jar org.apache.storm.starter.RollingTopWords
     *
     *
     * # Runs in remote/cluster mode, with topology name "production-topology"
     * $ storm jar storm-starter-jar-with-dependencies.jar org.apache.storm.starter.RollingTopWords remote
     * ```
     *
     * @param args Second positional argument (optional) defines
     *             whether to run the topology locally ("local") or remotely, i.e. on a real cluster ("remote").
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        boolean runLocally = true;
        if (args.length >= 1 && args[0].equalsIgnoreCase("remote")) {
            runLocally = false;
        }

        TopMeetupTopicsTopology topMeetupTopicsTopology = new TopMeetupTopicsTopology();
        if (runLocally) {
            LOG.info("Running in local mode");
            topMeetupTopicsTopology.runLocally();
        } else {
            LOG.info("Running in remote (cluster) mode");
            topMeetupTopicsTopology.runRemotely();
        }
    }
}
