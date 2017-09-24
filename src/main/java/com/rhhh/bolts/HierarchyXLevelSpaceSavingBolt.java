package com.rhhh.bolts;
import com.clearspring.analytics.stream.StreamSummary;
import com.rhhh.RHHH;
import com.rhhh.RHHHSpaceSaving;
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

public class HierarchyXLevelSpaceSavingBolt implements IRichBolt {
    private OutputCollector collector;
    private StreamSummary<String> counters;
    private String ipAddress;
    private String[] ipAddressArray;
    private int Level;
    private int ips_received;
    private RHHHSpaceSaving rhhh_manager;

    public HierarchyXLevelSpaceSavingBolt(int level){
        if  (level > 4 || level <= 0)
            throw new IllegalArgumentException();
        Level = level;
        ips_received = 0;
        rhhh_manager = RHHHSpaceSaving.getInstance();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counters = new StreamSummary<>(rhhh_manager.getEpsilon());
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
        this.counters.offerReturnAll(ipAddress, 1);
        if(ips_received % this.rhhh_manager.getTheta() == 0){
            this.updateMainFlow();
        }
//        rhhh_manager.addEntryOnLevel(this.Level, ipAddress);
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

    public StreamSummary<String> getCounters(){ return counters;}

    private void updateMainFlow(){
        rhhh_manager.mergeCounters(counters, Level);
        this.counters = new StreamSummary<>(rhhh_manager.getEpsilon());
    }
}
