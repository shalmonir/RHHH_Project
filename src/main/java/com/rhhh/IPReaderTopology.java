package com.rhhh;

import com.rhhh.bolts.HierarchyXLevelSpaceSavingBolt;
import com.rhhh.bolts.ReporterBolt;
import com.rhhh.spouts.IPReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.FailedLoginException;

/**
 * Created by root on 9/24/17.
 */
public class IPReaderTopology {
    public static void main(String[] args){
        try {
            DBUtils.ConnectDB();
            DBUtils.createTablesForLevels();
        } catch (FailedLoginException e) {
            e.printStackTrace();
            System.out.println("Failed to connect to db");
            System.exit(1);
        }
        long startTime = System.currentTimeMillis();
        Logger topology_log = LoggerFactory.getLogger(IPReaderTopology.class);
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
        builder.setBolt("Reporter", new ReporterBolt()).shuffleGrouping("ip-reader-spout","Reporter");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RHHHTopology", config, builder.createTopology());
        RHHHSpaceSaving.getInstance().setQuery_frequency(500000); //todo: delete: for debug
        long endTime = System.currentTimeMillis();
        topology_log.info("RHHHTopology Finished. Total time taken = " + (endTime - startTime));
        DBUtils.disconnectDB();
    }
}
