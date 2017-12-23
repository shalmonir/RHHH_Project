package com.rhhh;

import com.rhhh.bolts.HierarchyXLevelSpaceSavingBolt;
import com.rhhh.bolts.ReporterBolt;
import com.rhhh.spouts.SnifferSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.FailedLoginException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import static com.rhhh.bolts.HierarchyXLevelSpaceSavingBolt.setQuery_frequency;

/**
 * Created by root on 9/24/17.
 */
public class SnifferTopology  {
    public static void main(String[] args) throws FileNotFoundException {
        try {
            DBUtils.ConnectDB();
            DBUtils.createTablesForLevels();
        } catch (FailedLoginException e) {
            e.printStackTrace();
            System.out.println("Failed to connect to db");
            System.exit(1);
        }
        Logger topology_log = LoggerFactory.getLogger(SnifferTopology.class);
        topology_log.info("RHHHTopology started");
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ip-reader-spout", new SnifferSpout());
        builder.setBolt("level-1", new HierarchyXLevelSpaceSavingBolt(1)).shuffleGrouping("ip-reader-spout", "StreamForL1");
        builder.setBolt("level-2", new HierarchyXLevelSpaceSavingBolt(2)).shuffleGrouping("ip-reader-spout", "StreamForL2");
        builder.setBolt("level-3", new HierarchyXLevelSpaceSavingBolt(3)).shuffleGrouping("ip-reader-spout", "StreamForL3");
        builder.setBolt("level-4", new HierarchyXLevelSpaceSavingBolt(4)).shuffleGrouping("ip-reader-spout", "StreamForL4");
        builder.setBolt("Reporter", new ReporterBolt()).shuffleGrouping("ip-reader-spout","Reporter");
        if(args.length != 0){
            HierarchyXLevelSpaceSavingBolt.setEpsilon(Integer.parseInt(args[0]));
            ReporterBolt.setTheta(Double.parseDouble(args[1]));
            setQuery_frequency(Integer.parseInt(args[2]));
        }
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RHHHTopology", config, builder.createTopology());
        DBUtils.disconnectDB();
    }
}
