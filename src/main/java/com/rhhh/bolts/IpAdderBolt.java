package com.rhhh.bolts;


import java.net.Inet4Address;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Created by Nir on 06/05/2017.
 */

public class IpAdderBolt implements IRichBolt {
    private OutputCollector collector;
    Map<String, Integer> counters;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();

        this.collector = collector;
    }

    public void execute(Tuple input) {
        Inet4Address CurrentIp = (Inet4Address)input.getValue(0);
        String SrcIp = CurrentIp.toString().replace("/","");
        if(!counters.containsKey(SrcIp))
            counters.put(SrcIp,1);
        else
            counters.put(SrcIp, 1 + counters.get(SrcIp));
        collector.emit(new Values(SrcIp));
        collector.ack(input);
    }

    public void cleanup() {
        System.out.println("===>real count:");
        for(Map.Entry<String, Integer> entry:counters.entrySet()){
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ipAddress"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
