
package com.rhhh;


import java.io.Serializable;
import java.util.*;


/**
 * Created by Nir on 01/06/2017.
 */


public class RHHH implements Serializable {

    private static RHHH manager = new RHHH();
    Map<Integer, Map<String,Long>> dbMap;
    private double theta = 0.2; // todo: get as input
    private int epsilon = 1000; // todo: get as input + build tables accordingly
    private int query_frequency = 1000; // todo: get as input + design accordingly
    int N = 0; // the sum of all ip addresses counted by the manager

    public static RHHH getInstance(){
        return manager;
    }

    /**
     * ?? we might take that param as int and divide by 100?? more user-friendly
     * @param theta - new value for threshold parameter
     */
    public void setTheta(double theta){
        if(theta < 0){
            // todo: write error to log
            return;
        }
        this.theta = theta;
    }

    public void setQuery_frequency(int query_frequency){
        if(query_frequency < 0){
            // todo: write error to log
            return;
        }
        this.query_frequency = query_frequency;
    }

    private RHHH(){
        dbMap = new HashMap();
        for (int i=1; i<=4;i++){
            dbMap.put(i,new HashMap());
        }
    }

    /**
     * update new ip record in the general data structure
     * @param level - the HHH level to update
     * @param srcAdd - the partial or full ip address
     */
    public void addEntryOnLevel(int level,String srcAdd){
        N++;
        Map<String,Long> levelDb = dbMap.get(level);
        incrementEntryByOne(levelDb, srcAdd, level);
        if(N % query_frequency == 0)
            this.queryHeavyHitters();
    }

    /**
     * given a ip updates the counter with respect to the HHH level
     * @param levelDb - the data structure (Space-saving) of the correct level
     * @param srcAdd - the partial or full ip address
     * @param level - the HHH level to update
     */
    private void incrementEntryByOne(Map<String, Long> levelDb, String srcAdd, int level) {
        if (levelDb.containsKey(srcAdd)){
            levelDb.put(srcAdd, levelDb.get(srcAdd) + 1);
        } else {
            levelDb.put(srcAdd, (long)1);
        }
    }

    public Map<String,Long> getHeavyHitters(){
        return getHeavyHitters(1);
    }

    private Map<String,Long> getHeavyHitters(int level){
        Map<String,Long> hhlist = new HashMap<String, Long>();
        Map<String,Long> levelMap = dbMap.get(level);
        sortMap(levelMap);
        if (level == 4){
            for(Map.Entry<String,Long> entry : levelMap.entrySet()){
                if(entry.getValue() >= N * theta){
                    hhlist.put(entry.getKey(), entry.getValue());
                }
                else
                {
                    //Assuming map is sorted from the most HH to the least HH
                    break;
                }
            }
//            return hhlist; // todo: replace that call in normal one as soon as Saving-Space is integrated
            return sortMap(hhlist);
        }
        Map<String,Long> prevHH = getHeavyHitters(level+1);
        for (Map.Entry<String,Long> entry : levelMap.entrySet()){
            Long numOfTransportInSubHH = 0L;
            Long prefixHits = entry.getValue(); //counter ONLY in the current level
            String prefix = entry.getKey();
            if (prefixHits > N * theta){
                for(Map.Entry<String,Long> hh : prevHH.entrySet()){
                    if(isPrefixOf(prefix, hh.getKey())){
                        numOfTransportInSubHH += hh.getValue();
                    }
                }
                if (prefixHits - numOfTransportInSubHH > N * theta){
                    hhlist.put(entry.getKey(), prefixHits - numOfTransportInSubHH);
                }
            }
            else
            {
                //Assuming map is sorted from the most HH to the least HH
                continue;
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

    public static void main(String a[]){
        System.out.print("RHHH");
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
}
