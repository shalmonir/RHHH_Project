package com.rhhh;

import com.rhhh.bolts.HierarchyXLevelSpaceSavingBolt;
import com.rhhh.bolts.ReporterBolt;
import com.rhhh.spouts.SnifferSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.security.auth.login.FailedLoginException;

/**
 * Created by root on 27/09/17.
 */
    public class AbstractRhhhTopology {
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
            Logger topology_log = LoggerFactory.getLogger(AbstractRhhhTopology.class);
            topology_log.info(AbstractRhhhTopology.class.toString() + "Started");
            Config config = new Config();
            config.setDebug(true);
            config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

            TopologyBuilder builder = new TopologyBuilder();
            setSpout(builder);
            builder.setSpout("ip-reader-spout", new SnifferSpout());
            builder.setBolt("level-1", new HierarchyXLevelSpaceSavingBolt(1)).shuffleGrouping("ip-reader-spout", "StreamForL1");
            builder.setBolt("level-2", new HierarchyXLevelSpaceSavingBolt(2)).shuffleGrouping("ip-reader-spout", "StreamForL2");
            builder.setBolt("level-3", new HierarchyXLevelSpaceSavingBolt(3)).shuffleGrouping("ip-reader-spout", "StreamForL3");
            builder.setBolt("level-4", new HierarchyXLevelSpaceSavingBolt(4)).shuffleGrouping("ip-reader-spout", "StreamForL4");
            builder.setBolt("Reporter", new ReporterBolt()).shuffleGrouping("level-1")
                    .shuffleGrouping("level-2").shuffleGrouping("level-3")
                    .shuffleGrouping("level-4");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("RHHHTopology", config, builder.createTopology());
            RHHHSpaceSaving.getInstance().setQuery_frequency(3); //todo: delete: for debug
            long endTime = System.currentTimeMillis();
            topology_log.info("RHHHTopology Finished. Total time taken = " + (endTime - startTime));
            DBUtils.disconnectDB();
        }

    private static void setSpout(TopologyBuilder builder) {
            throw new NotImplementedException();
    }
}
