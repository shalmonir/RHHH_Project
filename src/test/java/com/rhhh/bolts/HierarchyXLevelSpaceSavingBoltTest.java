package com.rhhh.bolts;

import com.clearspring.analytics.stream.Counter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MockTupleHelpers;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Random;

import static org.mockito.Mockito.*;

/**
 * Created by Nir on 03/06/2017.
 */
public class HierarchyXLevelSpaceSavingBoltTest {
    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
    private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

    private Tuple mockIpTuple() {
        Random rand = new Random();
        Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
        String ip = rand.nextInt(256) + "" + rand.nextInt(256) + "" + rand.nextInt(256) + "" + rand.nextInt(256) + "";
        when(tuple.getValue(0)).thenReturn(ip);
        return tuple;
    }

    private Tuple FixIpTuple(String ip) {
        Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
        when(tuple.getValue(0)).thenReturn(ip);
        return tuple;
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void illegalLevelNumber() {
        Random rand = new Random();
        new HierarchyXLevelSpaceSavingBolt(rand.nextInt(10000) + 4);
    }

    @Test
    public void shouldAckIfTupleIsReceived() {
        Tuple normalTuple = mockIpTuple();
        OutputCollector collector = mock(OutputCollector.class);
        HierarchyXLevelSpaceSavingBolt bolt = new HierarchyXLevelSpaceSavingBolt(1);
        bolt.prepare(new HashMap(), null, collector);
        bolt.execute(normalTuple);
        verify(collector).ack(normalTuple);
    }

    @Test
    public void CountTupleCorrectly() {
        HierarchyXLevelSpaceSavingBolt bolt = new HierarchyXLevelSpaceSavingBolt(4);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(new HashMap(), null, collector);

        int tuple_amount = new Random().nextInt(300) + 5;
        for(int i = 0 ; i < tuple_amount ; i++){
            bolt.execute(FixIpTuple("1.2.3.4"));
        }
        assert (1 == bolt.getCounters().topK(1).size());
        assert (tuple_amount == bolt.getCounters().topK(1).get(0).getCount());
    }

    @Test
    public void parsingCorrectly() {
        for(int i = 1 ; i < 5 ; i++){
            HierarchyXLevelSpaceSavingBolt bolt = new HierarchyXLevelSpaceSavingBolt(i);
            OutputCollector collector = mock(OutputCollector.class);
            bolt.prepare(new HashMap(), null, collector);
            bolt.execute(FixIpTuple("1.1.1.1"));
            String ip_regex = "1";
            for(int j = 1 ; j < i ; j++){
                ip_regex = ip_regex + "\\.1" ;
            }
            for(Counter<String> entry : bolt.getCounters().topK(5)){
                if(!entry.getItem().matches(ip_regex))
                    throw new IllegalStateException("For level " + i + ": failed to parse tuple " + entry.getItem()+
                            " to pattern " + ip_regex);
            }
        }
    }

    @Test
    public void cleanupCorrectly() {
        OutputCollector collector = mock(OutputCollector.class);
        HierarchyXLevelSpaceSavingBolt bolt = new HierarchyXLevelSpaceSavingBolt(3);
        bolt.prepare(new HashMap(), null, collector);
        bolt.cleanup();
        verify(collector).emit(any(Values.class));
    }
}
