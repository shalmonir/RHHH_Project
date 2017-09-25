package com.rhhh;
import com.rhhh.bolts.HierarchyXLevelSpaceSavingBolt;
import com.rhhh.spouts.IPReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RHHHTopology {

    public static void main(String[] args) throws Exception{
        long startTime = System.currentTimeMillis();
        Logger topology_log = LoggerFactory.getLogger(RHHHTopology.class);
        topology_log.info("RHHHTopology started");
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ip-reader-spout", new IPReaderSpout(true, args));
        builder.setBolt("level-1", new HierarchyXLevelSpaceSavingBolt(1)).shuffleGrouping("ip-reader-spout", "StreamForL1");
        builder.setBolt("level-2", new HierarchyXLevelSpaceSavingBolt(2)).shuffleGrouping("ip-reader-spout", "StreamForL2");
        builder.setBolt("level-3", new HierarchyXLevelSpaceSavingBolt(3)).shuffleGrouping("ip-reader-spout", "StreamForL3");
        builder.setBolt("level-4", new HierarchyXLevelSpaceSavingBolt(4)).shuffleGrouping("ip-reader-spout", "StreamForL4");
        RHHHSpaceSaving.getInstance().setTheta(0.01);
        RHHHSpaceSaving.getInstance().setQuery_frequency(100L);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RHHHTopology", config, builder.createTopology());
        Thread.sleep(100000);

        long endTime = System.currentTimeMillis();
        topology_log.info("RHHHTopology Finished. Total time taken = " + (endTime - startTime));
//        cluster.shutdown();
    }

}
