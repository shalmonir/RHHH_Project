package com.rhhh.spouts;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * Created by Nir on 08/05/2017.
 */
public class IPReaderSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    int counter;
    boolean files_flag;
    private int current_stream;
    private List<String> streams = Arrays.asList("StreamForL1", "StreamForL2", "StreamForL3", "StreamForL4");
    //    private Logger LOG;
    private static final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    String[] Ip_source_files;
    FileReader file_reader;
    private int current_file_index;
    String line;
    BufferedReader buffer;

    public IPReaderSpout(boolean is_files_based, String[] input_files_list) {
        if (is_files_based == true && input_files_list == null) {
            throw new IllegalArgumentException();
        }
        files_flag = is_files_based;
        Ip_source_files = input_files_list;
//        LOG = LoggerFactory.getLogger(IPReaderSpout.class);
        current_file_index = 0;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("StreamForL1", new Fields("srcIP"));
        declarer.declareStream("StreamForL2", new Fields("srcIP"));
        declarer.declareStream("StreamForL3", new Fields("srcIP"));
        declarer.declareStream("StreamForL4", new Fields("srcIP"));

    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        Date date = new Date();
        this.collector = collector;
        counter = 0;
        current_stream = 0;
        line = null;
        System.out.println("===> Spout Start time = " + dateFormat.format(date));
    }

    /**
     * submit the next IP source address that is read from th Pcap file
     */
    public void nextTuple() {
        while (line == null) {
            open_next_file();
        }
        this.collector.emit(streams.get(current_stream), new Values(line));
        current_stream = (current_stream + 1) % 4;
        counter++;
        try {
            line = buffer.readLine();
        } catch (IOException e) {
            System.out.println("===>Caught IOException when reading file" + Ip_source_files[current_file_index]);
            e.printStackTrace();
            open_next_file();
        }
    }

    public void close() {
        Date date = new Date();
        System.out.println("===> Spout Finish time = " + dateFormat.format(date));
        System.out.println("===> IPReaderSpout Closed Input File. Counter = " + counter);
        // todo: temporary busy wait, find a better solution
        //while (1 > 0){}
        System.exit(0);
    }

    public boolean isDistributed() {
        return true;
    }

    public void activate() {
    }

    public void deactivate() {
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
//        LOG.warn("===> Spout failed passing the tuple : retry" + msgId);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void open_next_file() {
        if(current_file_index == Ip_source_files.length)
            close();
        try {
            file_reader = new FileReader(Ip_source_files[current_file_index]);
            buffer = new BufferedReader(file_reader);
            line = buffer.readLine();
        } catch (FileNotFoundException e) {
            // todo: Should we ignore this or give a solution that is more projecting to user?
            System.out.println("===> Could not open txt file: " + Ip_source_files[current_file_index]
                    + " - moving to next file");
            current_file_index++;
            open_next_file();
        } catch (IOException e) {
            System.out.println("===>Caught IOException - Trace:");
            e.printStackTrace();
            System.out.println("===>moving to next file");
            current_file_index++;
            open_next_file();
        }
    }
}
