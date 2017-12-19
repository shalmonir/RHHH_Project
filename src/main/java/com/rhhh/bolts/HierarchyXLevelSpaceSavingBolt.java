package com.rhhh.bolts;
import com.clearspring.analytics.stream.StreamSummary;
import com.rhhh.DBUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

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
    private String ThreadID;
    private Random rand;
    public static int updateDBFrequency = 100; //todo: return to 10000
    public static int epsilon = 1000;

    public static void setEpsilon(int eps){
        epsilon = eps;
    }

    public HierarchyXLevelSpaceSavingBolt(int level){
        if  (level > 4 || level <= 0)
            throw new IllegalArgumentException();
        Level = level;
        ips_received = 0;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counters = new StreamSummary<>(epsilon);
        this.collector = collector;
        ThreadID = Thread.currentThread().getName();    //for PID(same for all bolts): ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        rand = new Random();
    }

    public void execute(Tuple input) {
        try {
            if(!some_probability_func()){
                return;
            }
            ipAddress = "";
            ipAddressArray = input.getValue(0).toString().replace("/", "").split("\\.");
            int i = 0;
            while (i < Level - 1) {
                ipAddress = ipAddress + ipAddressArray[i++] + ".";
            }
            ipAddress = ipAddress + ipAddressArray[i];
            this.counters.offerReturnAll(ipAddress, 1);
            collector.ack(input);
            ips_received++;
            if (ips_received % updateDBFrequency == 0 && ips_received != 0) {
                this.updateMainFlow();
            }
        } catch (Exception e){
            e.printStackTrace();
            collector.fail(input);
        }
    }

    private boolean some_probability_func() {
        return rand.nextInt(10) == 1;
    }

    public void cleanup() {
        this.collector.emit(new Values(ips_received, counters));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public StreamSummary<String> getCounters(){ return counters;}

    private void updateMainFlow(){
        try {
            Connection conn = DriverManager.getConnection(DBUtils.RHHH_URL, DBUtils.USER, DBUtils.PASS);
            Statement stmt = conn.createStatement();
            String sql_cmd = "INSERT INTO Level" + Level + " (HH, total) VALUES ('" + Arrays.toString(counters.toBytes()) + "', " + ips_received + ")";
            stmt.executeUpdate(sql_cmd);
        }  catch (Exception e) {
            e.printStackTrace();
        }
    }
}
