package com.rhhh.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Mockito.mock;

/**
 * Created by Nir on 04/06/2017.
 */
public class IPReaderSpoutTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void SpoutNoArgsInConstructorTest() {
        new IPReaderSpout(true, null);
    }

    @Test
    public void InvalidFileName(){
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        IPReaderSpout spout = new IPReaderSpout(true, new String[] {"not_existing_file.txt"});
        spout.open(new HashMap(), null, collector);
        spout.nextTuple();
        spout.nextTuple();
    }
 //check git
}
