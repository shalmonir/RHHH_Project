package com.rhhh;
import com.rhhh.bolts.HierarchyXLevelBolt;
import com.rhhh.spouts.IPReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class RHHHTopology {

    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ip-reader-spout", new IPReaderSpout(true, args));
        builder.setBolt("level-1", new HierarchyXLevelBolt(1)).shuffleGrouping("ip-reader-spout", "StreamForL1");
        builder.setBolt("level-2", new HierarchyXLevelBolt(2)).shuffleGrouping("ip-reader-spout", "StreamForL2");
        builder.setBolt("level-3", new HierarchyXLevelBolt(3)).shuffleGrouping("ip-reader-spout", "StreamForL3");
        builder.setBolt("level-4", new HierarchyXLevelBolt(4)).shuffleGrouping("ip-reader-spout", "StreamForL4");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RHHHTopology", config, builder.createTopology());
//        Thread.sleep(10000);
//        cluster.shutdown();
    }

}
