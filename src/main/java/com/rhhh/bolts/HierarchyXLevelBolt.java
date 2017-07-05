package com.rhhh.bolts;
import com.rhhh.RHHH;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Nir on 06/05/2017.
 */

public class HierarchyXLevelBolt implements IRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counters;
    private String ipAddress;
    private String[] ipAddressArray;
    private int Level;
    private int ips_received;
    private RHHH rhhh_manager;

    public HierarchyXLevelBolt(int level){
        if  (level > 4 || level <= 0)
            throw new IllegalArgumentException();
        Level = level;
        ips_received = 0;
        rhhh_manager = RHHH.getInstance();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counters = new HashMap<>();
        this.collector = collector;
    }

    public void execute(Tuple input) {
        ipAddress = "";
        ipAddressArray = input.getValue(0).toString().split("\\.");
        int i = 0;
        while(i < Level-1) {
            ipAddress = ipAddress + ipAddressArray[i++] + ".";
        }
        ipAddress = ipAddress + ipAddressArray[i];
        /* todo: delete this old functionality at all right places
        if(!counters.containsKey(ipAddress))
            counters.put(ipAddress,1);
        else
            counters.put(ipAddress, 1 + counters.get(ipAddress));
        ips_received++;
        */
        rhhh_manager.addEntryOnLevel(this.Level, ipAddress);
        collector.ack(input);
    }

    public void cleanup() {
        this.collector.emit(new Values(ips_received, counters));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ips_processed"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public Map<String, Integer> getCounters(){ return counters;}
}
