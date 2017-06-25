/*
package com.rhhh;


import java.util.Map;

*/
/**
 * Created by Nir on 01/06/2017.
 *//*


public class RHHH {

    private RHHH manager = new RHHH();

    private double theta = 0.1;

    int N = 0;

    public RHHH getInstance(){
        return manager;
    }

    Map<Integer, Map<String,Long>> dbMap;

    private RHHH(){
        dbMap = new HashMap();
        for (int i=1; i<=4;i++){
            dbMap.put(i,new HashMap());
        }
    }

    public void addEntryOnLevel(int level,String[] srcAdd){
        N++;
        Map<String,Long> levelDb = dbMap.get(level);
        incrementEntryByOne(levelDb,srcAdd,level);

    }

    public String[] getHeavyHitters(){
        return getHeavyHitters(1);
    }

    private Entry<String,Long>[] getHeavyHitters(int level){
        Entry<String,Long>[] hhlist = new Entry[];
        Map<String,Long> levelMap = dbMap.get(level);
        if (level == 4){
            for(Entry<String,Long> entry : levelMap){
                if(entry.value() > N * theta){
                    hhlist.add(entry.key(),entry.value());
                }
                else
                {
                    //Assuming map is sorted from the most HH to the least HH
                    break;
                }
            }
            return hhlist;
        }
        Entry<String,Long>[] prevHH = getHeavyHitters(level+1);
        for (Entry<String,Long> entry : levelMap){
            Long numOfTransportInSubHH = 0L;
            Long prefixHits = entry.value();
            String prefix = entry.key();
            if (prefixHits > N * theta){
                for(Entry<String,Long> hh : prevHH){
                    if(isPrefixOf(prefix,hh.key())){
                        numOfTransportInSubHH+=hh.value();
                    }
                }
                if (prefixHits - numOfTransportInSubHH > N*theta){
                    hhlist.add(entry);
                }
            }
            else
            {
                //Assuming map is sorted from the most HH to the least HH
                break;
            }
        }
        return hhlist;
    }
}*/
