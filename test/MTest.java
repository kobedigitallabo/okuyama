package test;

import java.util.*;
import java.io.*;
import java.net.*;

import org.okuyama.imdst.client.ImdstKeyValueClient;
import org.okuyama.base.lang.BatchException;

public class MTest extends Thread {
    private boolean endFlg = false;
    private boolean startFlg = false;
    public int threadNo = 0;
    public long execCounter = 0;
    private String prefix = "";

    public void run() {
        try {

            if (TestSockM.args[0].equals("1")) {
                int counter = 0;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = TestSockM.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.setValue("datasavekey_" + threadNo, "savedatavaluestr_" + threadNo);
                imdstKeyValueClient.setValue("datasavekey_test" + threadNo, "savedatavaluestr_test" + threadNo);
                imdstKeyValueClient.setValue("datasavekey2_" + threadNo, "savedatavaluestr2_" + threadNo);
                imdstKeyValueClient.setValue("datasavekey2_test" + threadNo, "savedatavaluestr2_test" + threadNo);


                String key = "datasavekey_" + prefix + "_" + threadNo + "_";
                String value= "savedatavaluestr_" + prefix + "_" + threadNo + "_";
                while(true){

                    imdstKeyValueClient.setValue(key + this.execCounter, value + this.execCounter);
                    this.execCounter++;
                }

            } else if (TestSockM.args[0].equals("2")) {
                int counter = 0;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = TestSockM.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.getValue("datasavekey_" + threadNo);
                imdstKeyValueClient.getValue("datasavekey_test" + threadNo);
                imdstKeyValueClient.getValue("datasavekey2_" + threadNo);
                imdstKeyValueClient.getValue("datasavekey2_test" + threadNo);

                String key = "datasavekey_" + prefix + "_" + threadNo + "_";

                while(true){
                    imdstKeyValueClient.getValue(key + this.execCounter);
                    this.execCounter++;
                }

            } else if (TestSockM.args[0].equals("3")) {
                int counter = 0;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = TestSockM.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.setValue("datasavekey_" + threadNo, "savedatavaluestr_" + threadNo);
                imdstKeyValueClient.setValue("datasavekey_test" + threadNo, "savedatavaluestr_test" + threadNo);
                imdstKeyValueClient.setValue("datasavekey2_" + threadNo, "savedatavaluestr2_" + threadNo);
                imdstKeyValueClient.setValue("datasavekey2_test" + threadNo, "savedatavaluestr2_test" + threadNo);


                String key = "datasavekey_" + prefix + "_" + threadNo + "_";
                String value= "savedatavaluestr_" + prefix + "_" + threadNo + "_";
                while(true){
                    imdstKeyValueClient.setValue(key + this.execCounter, value + this.execCounter);
                    this.execCounter++;
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setStartFlg(boolean flg) {
        this.startFlg = flg;
        if (flg == true) {
            this.endFlg = false;
        } else {
            this.endFlg = true;
        }
    }

    public long getExecCounter() {
        return this.execCounter;
    }

}
