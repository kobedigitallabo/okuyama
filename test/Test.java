package test;

import java.util.*;
import java.io.*;
import java.net.*;

import org.imdst.client.ImdstKeyValueClient;
import org.batch.lang.BatchException;

public class Test extends Thread {
    private boolean endFlg = false;
    private boolean startFlg = false;
    public int threadNo = 0;
    public long execCounter = 0;
    private String prefix = "";
    private int maxPrefix = 0;

    public void run() {
        try {

            if (TestSock.args[0].equals("1")) {
                int counter = 1;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = TestSock.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.setValue("Key1", "Value1");
                imdstKeyValueClient.setValue("Key2", "Value2");
                imdstKeyValueClient.setValue("Key3", "Value3");
                imdstKeyValueClient.setValue("Key4", "Value4");


                String key = "DataSaveKey";
                String value= "Value012345678901234567890123456789";
                while(true){

                    if(!imdstKeyValueClient.setValue(key + this.execCounter, value + this.execCounter)) {
                        System.out.println("Error");
                    }
                    this.execCounter++;
                }

            } else if (TestSock.args[0].equals("2")) {
                int counter = 1;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = TestSock.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.getValue("Key1");
                imdstKeyValueClient.getValue("Key2");
                imdstKeyValueClient.getValue("Key3");
                imdstKeyValueClient.getValue("Key4");

                String key = "DataSaveKey";
                //Randomクラスのインスタンス化
                Random rnd = new Random();


                while(true){
                    String[] ret = imdstKeyValueClient.getValue(key + rnd.nextInt(maxPrefix));
                    if (!ret[0].equals("true")) {
                        System.out.println("Data Not Found");
                    }

                    //if (this.execCounter % 1000 == 0) System.out.println(this.execCounter);
                    this.execCounter++;
                }

            } else if (TestSock.args[0].equals("3")) {
                int counter = 1;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = TestSock.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.getValue("Key1");
                imdstKeyValueClient.getValue("Key2");
                imdstKeyValueClient.getValue("Key3");
                imdstKeyValueClient.getValue("Key4");

                String key = "Key";
                //Randomクラスのインスタンス化
                Random rnd = new Random();


                while(true){
                    String[] ret = imdstKeyValueClient.getValue(key + rnd.nextInt(maxPrefix));
                    if (ret[0].equals("true")) {
                        System.out.println(ret[1]);
                    } else {
                        System.out.println("Data Not Found");
                    }
                    Thread.sleep(100);
                }
            } else if (TestSock.args[0].equals("4")) {
                int counter = 1;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = TestSock.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();


                String key = "DataSaveKey";
                String value= "Value012345678901234567890123456789";
                for (int i = this.maxPrefix; i < (this.maxPrefix + 100000); i++){

                    if(!imdstKeyValueClient.setValue(key + i, value + i)) {
                        System.out.println("Error");
                    }
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

    public void setMaxPrefix(int prefix) {
        this.maxPrefix = prefix;
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
