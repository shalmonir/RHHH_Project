
package com.rhhh;


import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.io.Serializable;
import java.util.*;


/**
 * Created by Nir on 01/06/2017.
 */


public class RHHHSpaceSaving implements Serializable {

    private static RHHHSpaceSaving manager = new RHHHSpaceSaving();
    Map<Integer, StreamSummary<String>> dbMap;
    // todo: decide default values!
    private double theta = 0.2; // theta / 100 = the percentage require to be HH
    private int epsilon = 1000; // Table size
    private Long query_frequency = 1000L; // interval for sync between bolt and singleton
    long N = 0; // the sum of all ip addresses counted by the manager

    private RHHHSpaceSaving(){
        dbMap = new HashMap();
        for (int i=1; i<=4;i++){
            dbMap.put(i,new StreamSummary<String>(epsilon));
        }
    }

    public static RHHHSpaceSaving getInstance(){
        return manager;
    }

    private Map<String,Long> getHeavyHitters(int level){
        Map<String,Long> hhlist = new HashMap<>();
        StreamSummary<String> levelMap = dbMap.get(level);
        if (level == 4){
            for(Counter<String> entry : levelMap.topK(epsilon)){
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
        for (Counter<String> entry : levelMap.topK(epsilon)){
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
                    hhlist.put(entry.getItem(), prefixHits - numOfTransportInSubHH);
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

    /**
     *  return true iff key == prefix.*
     * @param prefix
     * @param key
     * @return
     */
    private boolean isPrefixOf(String prefix, String key) {
        if(key.startsWith(prefix))
            return true;
        else
            return false;
    }

    public void queryHeavyHitters(){
        Set<String> HH_set = getHeavyHitters().keySet();
        if(HH_set.isEmpty()){
            return;
        }
        System.out.print("~~~~ The HH in network traffic: \n");
        for(String hh : HH_set)
            System.out.print(hh + "\n");
    }

    /**
     * sorting map DESCENDING
     * @param map - the map to sort
     * @return - sorted map
     */
   private static Map<String, Long> sortMap(Map<String, Long> map){
       List list = new LinkedList(map.entrySet());
       // Defined Custom Comparator here
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

    public void resetStatsForTesting(){
       N = 0;
        for (int i=1; i<=4;i++){
            dbMap.put(i,new StreamSummary<String>(epsilon));
        }
    }

    public void mergeCounters(StreamSummary<String> counters, int level) {
        StreamSummary<String> main_stream = dbMap.get(level);
        int stream_size = counters.size();
        List<Counter<String>> new_values = counters.topK(stream_size);
        long stream_sum = 0;
        for (Counter<String> c : new_values) {
            main_stream.offerReturnAll(c.getItem(), (int)c.getCount()); //todo: change increment to long?
            stream_sum += c.getCount();

        }
        N += stream_sum;
    }

    /**
     * ?? we might take that param as int and divide by 100?? more user-friendly
     * @param theta - new value for threshold parameter
     */
    public void setTheta(double theta){
        if(theta < 0 || query_frequency < 0){
            // todo: write error to log
            return;
        }
        this.theta = theta;
    }

    public void setQuery_frequency(Long query_frequency){
        if(query_frequency < 0){
            // todo: write error to log
            return;
        }
        this.query_frequency = query_frequency;
    }

    public void setQuery_frequency(int query_frequency){
        if(query_frequency < 0){
            // todo: write error to log
            return;
        }
        this.query_frequency = (long)query_frequency;
    }

    public void setEpsilon(int epsilon){
        if(epsilon < 0){
            // todo: write error to log
            return;
        }
        this.epsilon = epsilon;
    }

    public int getEpsilon() {
        return epsilon;
    }

    public double getTheta() {
        return theta;
    }

    public Long getQueryFrequency() { return query_frequency; }

    public Map<String,Long> getHeavyHitters(){
        return getHeavyHitters(1);
    }
}
