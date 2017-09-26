package com.rhhh.bolts;

import com.clearspring.analytics.stream.StreamSummary;
import com.rhhh.DBUtils;
import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Created by root on 9/26/17.
 */
public class ReporterBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Connection conn = DriverManager.getConnection(DBUtils.RHHH_URL, DBUtils.USER, DBUtils.PASS);
            Statement stmt = conn.createStatement();
            //String sqlSelectTotalLevelCounter = "SELECT LAST 1 total FROM Level";
            for (int i = 1; i <= 4; i++) {
                String sqlSelectHHStreamCommand = "SELECT HH FROM Level"+i+" ORDER BY id desc LIMIT 1";
                ResultSet res = stmt.executeQuery(sqlSelectHHStreamCommand);
                res.next();
                String byteArrayAsString = res.getString(1);
                String[] byteArrayAsStringArray = byteArrayAsString.substring(1,byteArrayAsString.length()-1).split(",");
                int length = byteArrayAsStringArray.length;
                byte[] byteArray = new byte[length];
                for (int j = 0; j < length; j++) {
                    byteArray[j] = Byte.parseByte(byteArrayAsStringArray[j].trim());
                }
                StreamSummary<String> stream = new StreamSummary<>(byteArray);
                System.out.println(stream);
            }

        }
        catch (SQLException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
