package fuse.okuyamafs;

import java.util.*;
import java.io.*;

import okuyama.imdst.client.*;


/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class FilesystemCheckDaemon extends Thread {

    private int jobType = 0;
    private Object param = null;
    

    public FilesystemCheckDaemon(int jobType, Object param) {
        this.jobType = jobType;
        this.param = param;
    }

    public void run() {

        int baseBufferedQueue = OkuyamaFilesystem.blockSizeAssist;

        if (this.jobType==1 ) {
            String path = (String)this.param;
            while (true) {
                try {
                    if (OkuyamaFilesystem.jvmShutdownStatus == true) break;

                    Thread.sleep(10000);
                    File dir = new File(path);
                    dir.listFiles();
                    File tmpFile = new File(path + ".okuyamaFs_Check-Tmp");

                    FileWriter fw = new FileWriter(tmpFile);
                    StringBuilder tmpStr = new StringBuilder(8192);

                    for (int i = 0; i < 8192; i++) {
                        tmpStr.append("a");
                    }
                    fw.write(tmpStr.toString());
                    fw.flush();

                    FileReader fr = new FileReader(tmpFile);
                    fr.read(new char[8192]);

                    fw.close();
                    fr.close();

                    tmpFile.delete();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else if (this.jobType == 2) {
            while (true) {
                try {
                    if (OkuyamaFilesystem.jvmShutdownStatus == true) break;

                    Thread.sleep(1000);
                    int nowQueueJob = DelayStoreDaemon.nowQueueJob.get();
                    if (nowQueueJob > (OkuyamaFsMap.allDelaySJobSize * 0.7)) {

                        OkuyamaFilesystem.blockSizeAssist = 1;

                    } else if (nowQueueJob <= (OkuyamaFsMap.allDelaySJobSize * 0.5)) {

                        OkuyamaFilesystem.blockSizeAssist = baseBufferedQueue;
                    } 
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}


