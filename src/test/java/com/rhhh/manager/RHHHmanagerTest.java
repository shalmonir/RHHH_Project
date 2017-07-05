package com.rhhh.manager;

import com.rhhh.RHHH;
import org.junit.Test;

import java.util.HashMap;
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
        Map<String,Long> res = new HashMap<>();
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
        for(int i=0 ; i < 1000; i ++){

        }
    }
}
