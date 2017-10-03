package com.rhhh;

import java.io.*;
import java.sql.Time;
import java.text.SimpleDateFormat;

/**
 * Created by root on 29/09/17.
 */
public class StatisticsCollector {
    private String[] files;
    private double[] thetas;
    private int[] epsilons;
    private RHHHSpaceSaving RHHH;
    private long[] query_frequencies;
    private static final String latest = "latest.txt";


    public StatisticsCollector(String[] files, double[] thetas, int[] epsilons, long[] query_frequencies){
        this.thetas = thetas;
        this.epsilons = epsilons;
        this.files = files;
        this.query_frequencies = query_frequencies;
        RHHH = RHHHSpaceSaving.getInstance();
    }

    public void run_topology(){
        for(double theta: this.thetas){
            for(int epsilon: this.epsilons){
                for(long frequency: query_frequencies) {
                    RHHH.setTheta(theta);
                    RHHH.setEpsilon(epsilon);
                    RHHH.setQuery_frequency(frequency);
                    new IPReaderTopology().main(files);
                    try {
                        // 16 min sleep
                        Thread.sleep(2000000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    save_run_records(files, theta, epsilon, frequency);
                }
            }
        }
    }

    private void save_run_records(String[] files, double theta, int epsilon, long frequency) {
        try {
            String time_stamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
            PrintWriter writer = new PrintWriter(time_stamp + ".txt", "UTF-8");
            writer.println("Record of StatisticsCollector for configuration: \ntheta = " + theta + "\nepsilon = " +
                    epsilon + "\nquery frequency = " + frequency + "\nfiles:\n");
            for(String file : files) {
                writer.println(file);
            }
            writer.println("\nRHHH result for this run:\n");
            BufferedReader reader = new BufferedReader(new FileReader(latest));
            String line = reader.readLine();
            while (line != null){
                writer.println(line);
                line = reader.readLine();
            }
            reader.close();
            writer.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // issue with fileWriter
            e.printStackTrace();
        }
    }

    public static void main(String[] files){
        double[] thetas = {0.1, 0.2, 0.4, 0.6};
        int[] epsilons = {100, 1000};
        long[] queries = {4000, 40000};
        String[] file = {"/media/nir/C26606946606897D/Real_Trace/equinix-chicago.dirA.20160121-125911.UTC.anon.pcap.txt"};
        StatisticsCollector collector = new StatisticsCollector(file, thetas, epsilons, queries);
        collector.run_topology();
    }

    public static void everyPacketQuery(String[] files){
        double[] thetas = {0.001, 0.1, 0.2, 0.4, 0.6};
        int[] epsilons = {100, 1000};
        long[] queries = {4000, 40000};
        String[] file = {"/media/nir/C26606946606897D/Real_Trace/equinix-chicago.dirA.20160121-125911.UTC.anon.pcap.txt"};
    }
}
