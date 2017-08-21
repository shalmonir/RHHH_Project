package com.rhhh.manager;

import com.rhhh.RHHH;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;

/**
 * Created by Nir on 04/07/2017.
 * ISSUE - testing singleton is very problematic
 */
public class RHHHmanagerTest {
    @After
    public void beforeTestMethod() {
        RHHH.getInstance().resetStatsForTesting();
    }

    @Test
    public void oneIpTest() {
        RHHH rhhh = RHHH.getInstance();
        rhhh.addEntryOnLevel(4, "1.1.1.1");
    }

    @Test
    public void getHeavyHittersSmallTest() {
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.5);
        rhhh.setQuery_frequency(1);
        rhhh.addEntryOnLevel(4, "1.1.1.1");
        rhhh.addEntryOnLevel(3, "1.1.1");
        Map<String,Long> res;
        res = rhhh.getHeavyHitters();
        assert ((res.containsKey("1.1.1.1")));
        assert (!res.containsKey("1.1.1"));
        rhhh.addEntryOnLevel(4, "2.2.2.2");
        rhhh.addEntryOnLevel(3, "2.2.2");
        rhhh.addEntryOnLevel(4, "3.3.3.3");
        rhhh.addEntryOnLevel(3, "3.3.3");
        res = rhhh.getHeavyHitters();
        assert (res.isEmpty());
    }

    @Test
    public void getHeavyHittersBigTest() {
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.5);
        rhhh.setQuery_frequency(1);
        for(int i = 0 ; i < 10000000; i ++){
            rhhh.addEntryOnLevel(4, "1.1.1.1");
            rhhh.addEntryOnLevel(4, "1.1.1.2");
            rhhh.addEntryOnLevel(4, "1.1.1.3");
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.isEmpty());
    }

    @Test
    public void getHeavyHittersTinyTest() {
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.001);
        rhhh.setQuery_frequency(1);
        rhhh.addEntryOnLevel(4, "1.1.1.1");
        rhhh.addEntryOnLevel(3, "1.1.1");
        rhhh.addEntryOnLevel(2, "1.1");
        rhhh.addEntryOnLevel(1, "1");
        rhhh.addEntryOnLevel(4, "1.1.1.1");
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (!res.isEmpty());
    }

    @Test
    public void HHInLevel3Test() {
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.3);
        rhhh.setQuery_frequency(5);
        rhhh.addEntryOnLevel(4, "1.1.1.1");
        rhhh.addEntryOnLevel(4, "1.1.1.2");
        rhhh.addEntryOnLevel(4, "1.1.1.3");
        rhhh.addEntryOnLevel(4, "1.1.1.4");
        rhhh.addEntryOnLevel(4, "1.1.1.5");
        rhhh.addEntryOnLevel(3, "1.1.1");
        rhhh.addEntryOnLevel(3, "1.1.1");
        rhhh.addEntryOnLevel(3, "1.1.1");
        rhhh.addEntryOnLevel(3, "1.1.1");
        rhhh.addEntryOnLevel(3, "1.1.1");
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.containsKey("1.1.1"));
    }

    @Test
    public void HHInLevel1Test() {
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.3);
        rhhh.setQuery_frequency(5);
        for(int i = 0; i < 5; i++){
            rhhh.addEntryOnLevel(4, "1.1.1." + i);
            rhhh.addEntryOnLevel(3, "1.1." + i);
            rhhh.addEntryOnLevel(2, "1." + i);
        }
        for(int i = 0; i < 7; i++){
            rhhh.addEntryOnLevel(1, "1");
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.containsKey("1"));
    }

    /*@Test
    public void sortMapTest(){
        Map<String, Long> map = new HashMap();
        Map<String, Long> map_sorted = new HashMap();
        Random rand = new Random();
        for(int i = 0; i < 10 ; i++){
            map.put("a" + i, (long) rand.nextInt());
        }
        RHHH rhhh = RHHH.getInstance();
        map_sorted = rhhh.sortMap(map);
        assert (!map_sorted.equals(null));
        Long[] arr = map_sorted.values().toArray(new Long[0]);
        for(int i = 0; i <  map_sorted.values().size() - 1 ; i++){
            assert (arr[i] >=  arr[i + 1]);
        }
    }*/

    @Test
    public void subtractSonsTest(){
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.0001);
        rhhh.setQuery_frequency(15);
        for(int i = 0; i < 50; i++){
            rhhh.addEntryOnLevel(4, "1.1.1." + i);
            rhhh.addEntryOnLevel(3, "1.1.1");
        }
        Map<String,Long> res = rhhh.getHeavyHitters(); // ip "1.1.1" in level 3 should have 50 - 50 = 0 hits => no HH
        assert (!res.containsKey("1.1.1"));
    }

    @Test
    public void subtractSonsAndBeHHTest(){
        RHHH rhhh = RHHH.getInstance();
        rhhh.setTheta(0.3);
        rhhh.setQuery_frequency(1000);
        for(int i = 0; i < 50; i++){
            rhhh.addEntryOnLevel(4, "1.1.1.1");
        }
        for(int i = 0; i < 200; i++){
            rhhh.addEntryOnLevel(3, "1.1.1");
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        // ip "1.1.1" in level 3 should have 200 - 50 = 150 > 250 * 0.3 = 75 hits => HH
        assert (res.containsKey("1.1.1"));
        // ip "1.1.1.1" in level 4 should have 50 - 0 = 50 < 75 hits => no HH
        assert (!res.containsKey("1.1.1.1"));
    }
}
