package com.rhhh.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.pcap4j.core.*;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.util.NifSelector;

import java.io.EOFException;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by root on 9/24/17.
 */
public class SnifferSpout implements IRichSpout {

    PcapHandle handle;
    long counter;
    Inet4Address srcAdr;
    Inet4Address dstAdr;

    private SpoutOutputCollector collector;
    private int current_stream = 0;
    private List<String> streams = Arrays.asList("StreamForL1", "StreamForL2", "StreamForL3", "StreamForL4");

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        PcapNetworkInterface nif = null;
        try {
            nif = new NifSelector().selectNetworkInterface();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int snapLen = 100;
        PcapNetworkInterface.PromiscuousMode mode = PcapNetworkInterface.PromiscuousMode.PROMISCUOUS;
        int timeout = 10000;
        try {
            handle = nif.openLive(snapLen,mode,timeout);
        } catch (PcapNativeException e) {
            e.printStackTrace();
        }
        try {
            handle.setFilter("", BpfProgram.BpfCompileMode.OPTIMIZE);
        } catch (PcapNativeException e) {
            e.printStackTrace();
        } catch (NotOpenException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        Packet p = null;
        try {
            p = handle.getNextPacketEx();
            counter++;
            System.out.println(counter);
            IpV4Packet v4 = p.get(IpV4Packet.class);
            if (v4 != null) {
                srcAdr = v4.getHeader().getSrcAddr();
                if (srcAdr.isAnyLocalAddress()){
                    return;
                }
                dstAdr = v4.getHeader().getDstAddr();
                this.collector.emit(streams.get(current_stream), new Values(srcAdr.toString()));
                current_stream = (current_stream + 1) % 4;
            }
        } catch (PcapNativeException e) {
            e.printStackTrace();
        } catch (EOFException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (NotOpenException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("StreamForL1", new Fields("srcIP"));
        outputFieldsDeclarer.declareStream("StreamForL2", new Fields("srcIP"));
        outputFieldsDeclarer.declareStream("StreamForL3", new Fields("srcIP"));
        outputFieldsDeclarer.declareStream("StreamForL4", new Fields("srcIP"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
