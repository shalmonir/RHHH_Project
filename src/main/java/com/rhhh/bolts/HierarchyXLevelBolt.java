package com.rhhh.bolts;
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

public class HierarchyXLevelBolt implements IRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counters;
    private String ipAddress;
    private String[] ipAddressArray;
    private int Level;
    private int ips_received;

    public HierarchyXLevelBolt(int level){
        if  (level > 4 || level <= 0)
            throw new IllegalArgumentException();
        Level = level;
        ips_received = 0;
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
//        streamSummary.offer(ipAddress);
        if(!counters.containsKey(ipAddress))
            counters.put(ipAddress,1);
        else
            counters.put(ipAddress, 1 + counters.get(ipAddress));
        ips_received++;
        collector.ack(input);
    }

    public void cleanup() {
//        System.out.println("===>Hierarchy" + Level + "LevelBolt Total ip recored:" + ips_received);
//        for(Map.Entry<String, Integer> entry : counters.entrySet()){
//            System.out.println(entry.getKey()+" : " + entry.getValue());
//        }
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
