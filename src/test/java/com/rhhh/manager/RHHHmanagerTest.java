package com.rhhh.manager;

import com.rhhh.RHHH;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created by Nir on 04/07/2017.
 */
public class RHHHmanagerTest {

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

    // todo: there is a problem that when we run all the tests a once, the RHHH singleton remain - need to restart it between every 2 tests
    @Test
    public void nestedHHTest(){
        RHHH rhhh = RHHH.getInstance();
        rhhh.setQuery_frequency(1);
        rhhh.setTheta(0.1);
        for (int i = 0; i < 10; i++) {
            rhhh.addEntryOnLevel(4, "1.1.1.1");
            rhhh.addEntryOnLevel(3, "1.1.1");
            rhhh.addEntryOnLevel(3, "1.1.1");
        }
        for (int i = 0; i < 5; i++) {
            rhhh.addEntryOnLevel(4, "1.1.3.2");
            rhhh.addEntryOnLevel(3, "1.2.3");
            rhhh.addEntryOnLevel(3, "1.3.3");
            rhhh.addEntryOnLevel(3, "66.2.3");
            rhhh.addEntryOnLevel(4, "1.2.4.8");
            rhhh.addEntryOnLevel(3, "156.213.4");
            rhhh.addEntryOnLevel(2, "114.41");
            rhhh.addEntryOnLevel(3, "1.21.4");
            rhhh.addEntryOnLevel(3, "122.234.11");
            rhhh.addEntryOnLevel(3, "113.214.1");
            rhhh.addEntryOnLevel(3, "7.65.44");
        }
        Map<String, Long> res;
        res = rhhh.getHeavyHitters();
        /*
        N= 80
        Theta = 0.1
        theta * N = 8
        expected HH:
            1.1.1.1 - counter = 10
            1.1.1 - decrease generalized 1.1.1.1 (10) -> 20-10 = 10 > 8
         all other ips has 5 records and cause 5 < 8 -> NOT HH
         */
        Assert.assertEquals(2, res.keySet().size());
    }

    @Test
    public void changeThetaTest(){
        RHHH rhhh = RHHH.getInstance();
        rhhh.setQuery_frequency(10);
        rhhh.setTheta(0.50000000001);
        for (int i = 0; i < 10; i++) {
            rhhh.addEntryOnLevel(4, "1.1.1.1");
            rhhh.addEntryOnLevel(4, "1.1.1.2");
        }
        Map<String,Long> res;
        res = rhhh.getHeavyHitters();
        assert (res.isEmpty());
        rhhh.setTheta(0.5);
        res = rhhh.getHeavyHitters();
        assert (!res.isEmpty());
    }
}
