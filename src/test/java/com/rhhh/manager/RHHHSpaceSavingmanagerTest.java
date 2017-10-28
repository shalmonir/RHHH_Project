/*
package com.rhhh.manager;

import com.rhhh.bolts.HierarchyXLevelSpaceSavingBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.MockTupleHelpers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

*/
/**
 * Created by Nir on 04/07/2017.
 * ISSUE - testing singleton is very problematic
 *//*

public class RHHHSpaceSavingmanagerTest {
    HierarchyXLevelSpaceSavingBolt bolt1;
    HierarchyXLevelSpaceSavingBolt bolt2;
    HierarchyXLevelSpaceSavingBolt bolt3;
    HierarchyXLevelSpaceSavingBolt bolt4;
    RHHHSpaceSaving rhhh = RHHHSpaceSaving.getInstance();

    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
    private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

    private Tuple mockNormalTuple(Object obj) {
        Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
        when(tuple.getValue(0)).thenReturn(obj);
        return tuple;
    }

    @Before
    public void makeBasicStructure(){
        OutputCollector collector = mock(OutputCollector.class);
        bolt1 = new HierarchyXLevelSpaceSavingBolt(1);
        bolt2 = new HierarchyXLevelSpaceSavingBolt(2);
        bolt3 = new HierarchyXLevelSpaceSavingBolt(3);
        bolt4 = new HierarchyXLevelSpaceSavingBolt(4);
        bolt1.prepare(new HashMap(), null, collector);
        bolt2.prepare(new HashMap(), null, collector);
        bolt3.prepare(new HashMap(), null, collector);
        bolt4.prepare(new HashMap(), null, collector);
    }

    @After
    public void beforeTestMethod() {
        RHHHSpaceSaving.getInstance().resetStatsForTesting();
    }

    @Test
    public void oneIpTest() {
        OutputCollector collector = mock(OutputCollector.class);
        Tuple normalTuple = mockNormalTuple(new String("1.1.1.1"));
        bolt4.prepare(new HashMap(), null, collector);
        bolt4.execute(normalTuple);
    }

    @Test
    public void getHeavyHittersSmallTest() {
        rhhh.setTheta(0.5);
        rhhh.setQuery_frequency(1L);
        bolt4.execute(mockNormalTuple(new String("1.1.1.1")));
        bolt3.execute(mockNormalTuple(new String("1.1.1")));
        Map<String,Long> res;
        res = rhhh.getHeavyHitters();
        assert ((res.containsKey("1.1.1.1")));
        assert (!res.containsKey("1.1.1"));
        bolt4.execute(mockNormalTuple(new String("2.2.2.2")));
        bolt3.execute(mockNormalTuple(new String("2.2.2")));
        bolt4.execute(mockNormalTuple(new String("3.3.3.3")));
        bolt3.execute(mockNormalTuple(new String("3.3.3")));
        res = rhhh.getHeavyHitters();
        assert (res.isEmpty());
    }

    @Test
    public void getHeavyHittersBigTest() {
        rhhh.setTheta(0.5);
        rhhh.setQuery_frequency(1L);
        for(int i = 0 ; i < 10000; i ++){
            bolt4.execute(mockNormalTuple(new String("1.1.1.1")));
            bolt4.execute(mockNormalTuple(new String("1.1.1.2")));
            bolt4.execute(mockNormalTuple(new String("1.1.1.3")));
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.isEmpty());
    }

    @Test
    public void getHeavyHittersTinyTest() {
        rhhh.setTheta(0.001);
        rhhh.setQuery_frequency(1L);
        bolt4.execute(mockNormalTuple(new String("1.1.1.1")));
        bolt3.execute(mockNormalTuple(new String("1.1.1")));
        bolt2.execute(mockNormalTuple(new String("1.1")));
        bolt1.execute(mockNormalTuple(new String("1")));
        bolt4.execute(mockNormalTuple(new String("1.1.1.1")));
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (!res.isEmpty());
    }

    @Test
    public void HHInLevel3Test() {
        rhhh.setTheta(0.3);
        rhhh.setQuery_frequency(5);
        String[] listIps = {"1.1.1.1", "1.1.1.2", "1.1.1.3", "1.1.1.4", "1.1.1.5"};
        for(String ip : listIps){
            bolt4.execute(mockNormalTuple(ip));
            bolt3.execute(mockNormalTuple(ip));
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.containsKey("1.1.1"));
    }

    @Test
    public void HHInLevel1Test() {
        RHHHSpaceSaving rhhh = RHHHSpaceSaving.getInstance();
        rhhh.setTheta(0.3);
        rhhh.setQuery_frequency(5);
        for(int i = 0; i < 5; i++){
            bolt4.execute(mockNormalTuple(new String(  "1.1.1." + i)));
            bolt3.execute(mockNormalTuple(new String( "1.1." + i)));
            bolt2.execute(mockNormalTuple(new String( "1." + i)));
        }
        for(int i = 0; i < 7; i++){
            bolt1.execute(mockNormalTuple(new String( "1")));
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.containsKey("1"));
    }

//    @Test
//    public void sortMapTest(){
//        Map<String, Long> map = new HashMap();
//        Map<String, Long> map_sorted = new HashMap();
//        Random rand = new Random();
//        for(int i = 0; i < 10 ; i++){
//            map.put("a" + i, (long) rand.nextInt());
//        }
//        RHHHSpaceSaving rhhh = RHHHSpaceSaving.getInstance();
            //// for testing 'sortMap' should be public
//        map_sorted = rhhh.sortMap(map);
//        assert (!map_sorted.equals(null));
//        Long[] arr = map_sorted.values().toArray(new Long[0]);
//        for(int i = 0; i <  map_sorted.values().size() - 1 ; i++){
//            assert (arr[i] >=  arr[i + 1]);
//        }
//    }

    @Test
    public void subtractSonsTest(){
        RHHHSpaceSaving rhhh = RHHHSpaceSaving.getInstance();
        rhhh.setTheta(0.0001);
        rhhh.setQuery_frequency(15);
        for(int i = 0; i < 50; i++){
            bolt4.execute(mockNormalTuple(new String(  "1.1.1." + i)));
            bolt3.execute(mockNormalTuple(new String(  "1.1.1")));
        }
        Map<String,Long> res = rhhh.getHeavyHitters(); // ip "1.1.1" in level 3 should have 50 - 50 = 0 hits => no HH
        assert (!res.containsKey("1.1.1"));
    }

    @Test
    public void subtractSonsAndBeHHTest(){
        RHHHSpaceSaving rhhh = RHHHSpaceSaving.getInstance();
        rhhh.setTheta(0.3);
        rhhh.setQuery_frequency(10);
        for(int i = 0; i < 50; i++){
            bolt4.execute(mockNormalTuple(new String( "1.1.1.1")));
        }
        for(int i = 0; i < 200; i++){
            bolt3.execute(mockNormalTuple(new String( "1.1.1")));
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        // ip "1.1.1" in level 3 should have 200 - 50 = 150 > 250 * 0.3 = 75 hits => HH
        assert (res.containsKey("1.1.1"));
        // ip "1.1.1.1" in level 4 should have 50 - 0 = 50 < 75 hits => no HH
        assert (!res.containsKey("1.1.1.1"));
    }

    @Test
    public void checkParallelism(){
        RHHHSpaceSaving rhhh = RHHHSpaceSaving.getInstance();
        rhhh.setQuery_frequency(1);
        rhhh.setTheta(0.4);
        HierarchyXLevelSpaceSavingBolt bolt4_2 = new HierarchyXLevelSpaceSavingBolt(4);
        HierarchyXLevelSpaceSavingBolt bolt4_3 = new HierarchyXLevelSpaceSavingBolt(4);
        HierarchyXLevelSpaceSavingBolt bolt4_4 = new HierarchyXLevelSpaceSavingBolt(4);
        OutputCollector collector = mock(OutputCollector.class);
        bolt4_2.prepare(new HashMap(), null, collector);
        bolt4_3.prepare(new HashMap(), null, collector);
        bolt4_4.prepare(new HashMap(), null, collector);
        for(int i = 0; i < 10; i++){
            bolt4.execute(mockNormalTuple(new String( "1.1.1." + i)));
            bolt4_2.execute(mockNormalTuple(new String( "1.1.1." + (20 + i))));
            bolt4_3.execute(mockNormalTuple(new String( "1.1.1.1")));
            bolt4_4.execute(mockNormalTuple(new String( "1.1.1.1")));
        }
        Map<String,Long> res = rhhh.getHeavyHitters();
        assert (res.containsKey("1.1.1.1"));
    }
}
*/
