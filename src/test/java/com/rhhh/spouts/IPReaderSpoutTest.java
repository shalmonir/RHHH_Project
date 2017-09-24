package com.rhhh.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.testng.annotations.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Random;

import static org.mockito.Mockito.mock;

/**
 * Created by Nir on 04/06/2017.
 */
public class IPReaderSpoutTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void SpoutNoArgsInConstructorTest() {
        new IPReaderSpout(true, null);
    }

    // todo: implement in a way that unit test run
//    @Rule
//    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
//    @Test
//    public void InvalidFileName(){
//        exit.expectSystemExitWithStatus(1);
//        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
//        IPReaderSpout spout = new IPReaderSpout(true, new String[] {"not_existing_file.txt"});
//// todo: find solution for exit
////        spout.open(new HashMap(), null, collector);
//    }

    @Test
    public void FileInputTest() throws IOException {
        String path = System.getProperty("user.dir");
        String fileName = path + "\\ips_file.txt";

        FileWriter fw = new FileWriter(fileName);
        BufferedWriter bw = new BufferedWriter(fw);

        bw.write("1.1.1.1");
        bw.newLine();
        bw.write("1.1.1.2");
        bw.close();

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        IPReaderSpout spout = new IPReaderSpout(true, new String[] {fileName});
        spout.open(new HashMap(), null, collector);
        spout.nextTuple();
        spout.nextTuple();
    }

    @Test
    public void MultipleFileInputTest() throws IOException {
        String path = System.getProperty("user.dir");
        String fileName1 = path + "\\ips_file1.txt";
        String fileName2 = path + "\\ips_file2.txt";

        FileWriter fw1 = new FileWriter(fileName1);
        BufferedWriter bw1 = new BufferedWriter(fw1);
        FileWriter fw2 = new FileWriter(fileName2);
        BufferedWriter bw2 = new BufferedWriter(fw2);

        Random rand = new Random();
        for (int i = 0; i < 100; i++){
            bw1.write(rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256)
                    + "." + rand.nextInt(256));
            bw2.write(rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256)
                    + "." + rand.nextInt(256));
            bw1.newLine();
            bw2.newLine();
        }
        bw1.close();
        bw2.close();

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        IPReaderSpout spout = new IPReaderSpout(true, new String[] {fileName1, fileName2});
        spout.open(new HashMap(), null, collector);
        for (int i = 0; i < 200; i++){
            spout.nextTuple();
        }
        assert spout.is_finished_all_files() == false;
        // This will cause exit:
//        spout.nextTuple();
//        assert spout.is_finished_all_files() == true;
    }

}
