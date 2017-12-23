package com.rhhh.bolts;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import com.rhhh.DBUtils;
import com.rhhh.HtmlUtility;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

import static java.lang.System.exit;

/**
 * Created by root on 9/26/17.
 */
public class ReporterBolt implements IRichBolt {

    private OutputCollector collector;
    private Statement stmt = null;
    private int epsilon = HierarchyXLevelSpaceSavingBolt.epsilon;
    public static double theta = 0.01;
    private long N = 0;
    private PrintWriter writer;
    private StreamSummary<String>[] current_counters = null;
    long startTime;
    Connection conn;

    public static void setTheta(double theta){
        ReporterBolt.theta = theta;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        current_counters = new StreamSummary[5];
        try{
            File f = new File("/tmp/rhhh");
            if(!f.exists()){
                f.mkdir();
            }
            writer = new PrintWriter("/tmp/rhhh/Report.txt", "UTF-8");
            writer.println("Reporter report Started\n");
            PrintWriter writer_latest = new PrintWriter("/tmp/rhhh/Latest.txt", "UTF-8");
            writer_latest.println("");
        } catch (IOException e) {
            e.printStackTrace();
            exit(0);
        }
        try {
            PrintWriter writerHtml = new PrintWriter("/tmp/rhhh/display.html", "UTF-8");
            writerHtml.print(HtmlUtility.html);
            writerHtml.flush();
//            Files.copy(new File("display.html").toPath(), new File("/tmp/rhhh/display.html").toPath(), REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Failed to copy html file to your tmp/rhhh");
            e.printStackTrace();
            exit(0);
        }
        try {
            conn = DriverManager.getConnection(DBUtils.RHHH_URL, DBUtils.USER, DBUtils.PASS);
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            exit(0);
        }
        startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            conn = DriverManager.getConnection(DBUtils.RHHH_URL, DBUtils.USER, DBUtils.PASS);
            Map<String,Long> hhhmap = getHeavyHitters(1);
            if(hhhmap != null) {
                long timePast = (System.currentTimeMillis() - startTime) / 1000;
                writer.println("Total = " + N + "Time since started = " + timePast + ". HH list: " + hhhmap + "\n");
                writer.flush();
                String formatStr = "%-20s %-20s\n";
                PrintWriter writer_latest = new PrintWriter("/tmp/rhhh/Latest.txt", "UTF-8");
                writer_latest.println("Total Packages = " + N + ", Number of HH = "+ hhhmap.size() +
                        " , Time since started = " + timePast + " seconds.\n Here are the heavy hitters: \n");
                writer_latest.write(String.format(formatStr,"IP prefix", "Hits"));
                for(Map.Entry<String, Long> entry : hhhmap.entrySet()){
                    writer_latest.write(String.format(formatStr,entry.getKey(), entry.getValue()));
                }
                writer_latest.flush();
            }
        } catch (MySQLNonTransientConnectionException e){
            e.printStackTrace();
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
            if(current_counters[i] == null)
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
        return sortMap(tmp);
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


    /**
     * we calculate total and StreamSummary simultaneously to avoid mismatch of data (line added between calls)
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void getStreamSummaryForLevelAndUpdateTotal() throws SQLException, IOException, ClassNotFoundException {
        long sum = 0;
        for (int i = 1; i <= 4; i++) {
            String sqlSelectTotalStreamCommand = "SELECT total, HH FROM Level"+i+" ORDER BY id desc LIMIT 1";
            ResultSet res = stmt.executeQuery(sqlSelectTotalStreamCommand);
            res.next();
            sum += Long.parseLong(res.getString(1));
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

