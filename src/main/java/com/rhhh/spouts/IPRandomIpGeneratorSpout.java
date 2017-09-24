package com.rhhh.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by Nir on 08/05/2017.
 */
public class IPRandomIpGeneratorSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private List<String> streams = Arrays.asList("StreamForL1", "StreamForL2", "StreamForL3", "StreamForL4");
    private Logger spout_log = LoggerFactory.getLogger(IPRandomIpGeneratorSpout.class);
    private static final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    private long msgId = 0;
    private Random rand;
    private int current_stream;
    private String ip;

    public IPRandomIpGeneratorSpout() {
        spout_log.info("IPRandomIpGeneratorSpout created successfully");
        current_stream = 0;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("StreamForL1", new Fields("srcIP"));
        declarer.declareStream("StreamForL2", new Fields("srcIP"));
        declarer.declareStream("StreamForL3", new Fields("srcIP"));
        declarer.declareStream("StreamForL4", new Fields("srcIP"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        Date date = new Date();
        this.collector = collector;
        this.rand = new Random();
        spout_log.info("Spout Start time = " + dateFormat.format(date));
    }

    public void close() {
    }

    public void activate() {
    }

    public void deactivate() {
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    @Override
    public void nextTuple() {
        Utils.sleep(100);
        ip =rand.nextInt(255) + "." + rand.nextInt(255) + "." + rand.nextInt(255) + "." +
                rand.nextInt(255);
        this.collector.emit(streams.get(current_stream), new Values(ip));
    }

    @Override
    public void ack(Object msgId) {
        spout_log.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        spout_log.debug("Got FAIL for msgId : " + msgId);
    }
}
