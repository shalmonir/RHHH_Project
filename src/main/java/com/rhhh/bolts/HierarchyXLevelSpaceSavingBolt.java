package com.rhhh.bolts;
import com.clearspring.analytics.stream.StreamSummary;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
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

import static java.lang.System.exit;

/**
 * Created by Nir on 06/05/2017.
 */

public class HierarchyXLevelSpaceSavingBolt implements IRichBolt {
    private OutputCollector collector;
    private StreamSummary<String> counters;
    private String ipAddress;
    private String[] ipAddressArray;
    private int Level;
    public static boolean[] hasInitiatedDB;
    private int ips_received;
    public static long updateDBFrequency = 10000;
    public static int epsilon = 100;
    private static Connection conn;

    public static void setEpsilon(int eps){
        epsilon = eps;
    }

    public HierarchyXLevelSpaceSavingBolt(int level){
        if  (level > 4 || level <= 0)
            throw new IllegalArgumentException();
        Level = level;
        ips_received = 0;
        hasInitiatedDB = new boolean[4];
        Arrays.fill(hasInitiatedDB, Boolean.FALSE);
    }

    public static void setQuery_frequency(long query_frequency) {
        HierarchyXLevelSpaceSavingBolt.updateDBFrequency = query_frequency;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counters = new StreamSummary<>(epsilon);
        this.collector = collector;
        try {
            conn = DriverManager.getConnection(DBUtils.RHHH_URL, DBUtils.USER, DBUtils.PASS);
        } catch (SQLException e) {
            e.printStackTrace();
            exit(0);
        }
    }

    public void execute(Tuple input) {
        try {
            ipAddress = "";
            if(input == null) return;
            Object x = input.getValue(0);
            if(x == null) return;
            ipAddressArray = x.toString().replace("/", "").split("\\.");
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


    public void cleanup() {
        this.collector.emit(new Values(ips_received, counters));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void updateMainFlow(){
        try {
            Statement stmt = conn.createStatement();
            String sql_cmd = "";
            if(hasInitiatedDB[Level - 1]){
                sql_cmd = "UPDATE Level" + Level + " SET HH='" + Arrays.toString(counters.toBytes())+ "' , total=" + ips_received + " WHERE id=1";
            } else {
                hasInitiatedDB[Level - 1] = true;
                sql_cmd = "INSERT INTO Level" + Level + " (HH, total) VALUES ('" + Arrays.toString(counters.toBytes()) + "', " + ips_received + ")";
            }
            stmt.executeUpdate(sql_cmd);
        } catch (MySQLNonTransientConnectionException e){
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
