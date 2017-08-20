package com.rhhh;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * Created by nir.shalmon on 8/15/2017.
 */
public class GeneralTestForRealFile {
    @Test
    public void GetFileAndProcess() throws IOException {
        RHHH rhhh = RHHH.getInstance();
        String path = System.getProperty("user.dir");
        String fileName = path + "\\GetFileAndProcess.txt";
        FileWriter fw = new FileWriter(fileName);
        BufferedWriter bw = new BufferedWriter(fw);
        Random rand = new Random();
        for (int i = 0; i < 10000; i++) {
            bw.write(rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256)
                    + "." + rand.nextInt(256));
            bw.newLine();
            bw.write("255.255.255.255");
            bw.newLine();
        }
        try {
            RHHHTopology.main(new String[]{fileName});
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<String,Long> res;
        res = rhhh.getHeavyHitters();
        assert res.containsKey("255.255.255.255");
    }
}
