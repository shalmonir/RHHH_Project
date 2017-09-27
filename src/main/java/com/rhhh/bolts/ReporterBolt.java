package com.rhhh.bolts;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.rhhh.DBUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

/**
 * Created by root on 9/26/17.
 */
public class ReporterBolt implements IRichBolt {

    private OutputCollector collector;
    private Statement stmt = null;
    private int epsilon = 1000;
    private double theta = 0.001;
    private long N = 0;
    private PrintWriter writer;
    private StreamSummary<String>[] current_counters = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        current_counters = new StreamSummary[5];
        try{
            writer = new PrintWriter("Report.txt", "UTF-8");
            writer.println("Reporter report Started\n");
//            writer.close();
        } catch (IOException e) {
            // do something
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Connection conn = DriverManager.getConnection(DBUtils.RHHH_URL, DBUtils.USER, DBUtils.PASS);
            stmt = conn.createStatement();
//            N = getTotalPacketNumber();
            //String sqlSelectTotalLevelCounter = "SELECT LAST 1 total FROM Level";
            Map<String,Long> hhhmap = getHeavyHitters(1);
//            System.out.println("@@"+hhhmap);
            if(hhhmap != null)
                writer.println("Total = " + N + ". HH list: " + hhhmap + "\n");
        }
        catch (SQLException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Map<String,Long> getHeavyHitters(int level) throws SQLException, IOException, ClassNotFoundException {
        getStreamSummaryForLevelAndUpdateTotal();
        for (int i = 1; i < 5; i++) {
            if(current_counters[level] == null)
                return null;
        }
        Map<String,Long> hhlist = new HashMap<>();
        StreamSummary<String> levelSummary = current_counters[level];
        if (level == 4){
            for(Counter<String> entry : levelSummary.topK(epsilon)){
                if(entry.getCount() >= N * theta){
                    hhlist.put(entry.getItem(), entry.getCount());
                }
                else
                {
                    break;
                }
            }
            return sortMap(hhlist);
        }
        Map<String,Long> prevHH = getHeavyHitters(level+1);
        for (Counter<String> entry : levelSummary.topK(epsilon)){
            Long numOfTransportInSubHH = 0L;
            Long prefixHits = entry.getCount(); //counter ONLY in the current level
            String prefix = entry.getItem();
            if (prefixHits > N * theta){
                for(Map.Entry<String,Long> hh : prevHH.entrySet()){
                    if(isPrefixOf(prefix, hh.getKey())){
                        numOfTransportInSubHH += hh.getValue();
                    }
                }
                if (prefixHits - numOfTransportInSubHH > N * theta){
                    hhlist.put(entry.getItem(), prefixHits);// - numOfTransportInSubHH);
                }
            }
            else
            {
                //Assuming map is sorted from the most HH to the least HH
                break;
            }
        }
        Map<String,Long> tmp = new HashMap<>();
        tmp.putAll(prevHH);
        tmp.putAll(hhlist);
        return tmp;
    }

    private boolean isPrefixOf(String prefix, String key) {
        return key.startsWith(prefix);
    }

    /**
     * sorting map DESCENDING
     * @param map - the map to sort
     * @return - sorted map
     */
    private static Map<String, Long> sortMap(Map<String, Long> map){
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return (((Comparable) ((Map.Entry) (o2)).getValue())
                        .compareTo(((Map.Entry) (o1)).getValue()));
            }
        });
        HashMap sortedHashMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }

    private StreamSummary<String> getStreamSummaryForLevel(int level) throws SQLException, IOException, ClassNotFoundException {
        String sqlSelectHHStreamCommand = "SELECT HH FROM Level"+ level +" ORDER BY id desc LIMIT 1";
        ResultSet res = stmt.executeQuery(sqlSelectHHStreamCommand);
        res.next();
        String byteArrayAsString = res.getString(1);
        String[] byteArrayAsStringArray = byteArrayAsString.substring(1,byteArrayAsString.length()-1).split(",");
        int length = byteArrayAsStringArray.length;
        byte[] byteArray = new byte[length];
        for (int j = 0; j < length; j++) {
            byteArray[j] = Byte.parseByte(byteArrayAsStringArray[j].trim());
        }
        return new StreamSummary<>(byteArray);

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

    public long getTotalPacketNumber() throws SQLException {
        long sum = 0;
        for (int i = 1; i <= 4; i++) {
            String sqlSelectTotalStreamCommand = "SELECT total FROM Level"+i+" ORDER BY id desc LIMIT 1";
            ResultSet res = stmt.executeQuery(sqlSelectTotalStreamCommand);
            res.next();
            sum += Long.parseLong(res.getString(1));
        }
        return sum;
    }

    /**
     * Suggest we calculate total and StreamSummary simultaneously to avoid mismatch of data (line added between calls)
     * @return
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void getStreamSummaryForLevelAndUpdateTotal() throws SQLException, IOException, ClassNotFoundException {
        StreamSummary<String>[] allLevels = new StreamSummary[5];
        long sum = 0;
        for (int i = 1; i <= 4; i++) {
            String sqlSelectTotalStreamCommand = "SELECT total, HH FROM Level"+i+" ORDER BY id desc LIMIT 1";
            ResultSet res = stmt.executeQuery(sqlSelectTotalStreamCommand);
            res.next();
            sum += Long.parseLong(res.getString(1));
            // now, we get the counters back from byteArray:

//            String sqlSelectHHStreamCommand = "SELECT HH FROM Level"+ i +" ORDER BY id desc LIMIT 1";
//            res = stmt.executeQuery(sqlSelectHHStreamCommand);
//            res.next();

            String byteArrayAsString = res.getString(2);
            String[] byteArrayAsStringArray = byteArrayAsString.substring(1,byteArrayAsString.length()-1).split(",");
            int length = byteArrayAsStringArray.length;
            byte[] byteArray = new byte[length];
            for (int j = 0; j < length; j++) {
                byteArray[j] = Byte.parseByte(byteArrayAsStringArray[j].trim());
            }
            current_counters[i] = new StreamSummary<>(byteArray);
        }
        N = sum;
    }
}
