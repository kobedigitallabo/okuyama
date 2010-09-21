package test;

import java.util.*;
import java.io.*;
import java.net.*;

import org.imdst.client.ImdstKeyValueClient;
import org.batch.lang.BatchException;

public class Test extends Thread {

    public volatile int threadNo = 0;
    public volatile long execCounter = 0;

    private volatile String prefix = "";
    private volatile int maxPrefix = 0;

    private volatile boolean endFlg = false;

    public void run() {
        while(!TestSock.startFlg){}
        try {
            if (TestSock.args[0].equals("set")) {
                int counter = 1;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = TestSock.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.setValue("Key1", "Value1");
                imdstKeyValueClient.setValue("Key2", "Value2");
                imdstKeyValueClient.setValue("Key3", "Value3");
                imdstKeyValueClient.setValue("Key4", "Value4");

                Random rnd = new Random();

                String key = "DataSaveKey_" + threadNo + "_";
                String value= "Value012345678901234567890123456789_" + threadNo + "_";
                while(true &&  TestSock.startFlg){

                    if(!imdstKeyValueClient.setValue(key + rnd.nextInt(1000000), value)) {
                        System.out.println("Error");
                    }
                    this.execCounter++;
                }

            } else if (TestSock.args[0].equals("get")) {
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


                while(true &&  TestSock.startFlg){
                    String[] ret = imdstKeyValueClient.getValue(key + rnd.nextInt(maxPrefix));
                    if (!ret[0].equals("true")) {
                        System.out.println("Data Not Found");
                    }

                    //if (this.execCounter % 1000 == 0) System.out.println(this.execCounter);
                    this.execCounter++;
                }

            } else if (TestSock.args[0].equals("print")) {
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


                while(true &&  TestSock.startFlg){
                    String[] ret = imdstKeyValueClient.getValue(key + rnd.nextInt(maxPrefix));
                    if (ret[0].equals("true")) {
                        System.out.println(ret[1]);
                    } else {
                        System.out.println("Data Not Found");
                    }
                }
            } else if (TestSock.args[0].equals("4")) {
                int counter = 1;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = TestSock.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();


                String key = "DataSaveKey";
                String value= "Value012345678901234567890123456789";
                for (int i = this.maxPrefix; i < (this.maxPrefix + 20000); i++){

                    if(!imdstKeyValueClient.setValue(key + i, value + i)) {
                        System.out.println("Error");
                    }
                    this.execCounter++;
                }

            } else if (TestSock.args[0].equals("5")) {
                int counter = 1;
                boolean execMethodFlg =false;
                
                ImdstKeyValueClient imdstKeyValueClient = null;
                ArrayList clientList = new ArrayList();
                String[] infos = TestSock.args[1].split(",");

                System.out.println("Connect Start ThreadNo[" + this.threadNo + "]");
                for (int i = 0; i < this.maxPrefix; i++){

                    imdstKeyValueClient = new ImdstKeyValueClient();
                    imdstKeyValueClient.setConnectionInfos(infos);
                    imdstKeyValueClient.autoConnect();
                    clientList.add(imdstKeyValueClient);
                }

                System.out.println("Connect End ThreadNo[" + this.threadNo + "]");

                //Randomクラスのインスタンス化
                Random rnd = new Random();

                String key = "DataSaveKey";
                String value= "Value012345678901234567890123456789";

                if (this.threadNo == 0) {
                    execMethodFlg = true;
                } else if ((this.threadNo % 2) == 1) {
                    execMethodFlg = false;
                } else {
                    execMethodFlg = true;
                }

                for (int t = 0; t < 10; t++) {
                    for (int i = 0; i < clientList.size(); i++){

                        imdstKeyValueClient = (ImdstKeyValueClient)clientList.get(i);
                        if (execMethodFlg) {
                            String[] ret = imdstKeyValueClient.getValue(key + rnd.nextInt(1000000));
                            if (ret[0].equals("true")) {
                                //System.out.println(ret[1]);
                            } else {
                                System.out.println("Data Get Not Found");
                            }
                        } else {
                            if(!imdstKeyValueClient.setValue(key + rnd.nextInt(1000000), value + rnd.nextInt(1000000))) System.out.println("Data Set Error");
                        }
                        this.execCounter++;
                       
                    }
                }

                for (int i = 0; i < clientList.size(); i++){

                    imdstKeyValueClient = (ImdstKeyValueClient)clientList.get(i);
                    imdstKeyValueClient.close();
                }
                endFlg = true;
                System.out.println("Connect End Method End ThreadNo[" + this.threadNo + "]");
            }
        } catch (Exception e) {
            e.printStackTrace();
            endFlg = true;
        }
    }

    public void setThreadNo(int i) {
        this.threadNo = i;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setMaxPrefix(int prefix) {
        this.maxPrefix = prefix;
    }

    public long getExecCounter() {
        return this.execCounter;
    }

    public boolean getEndFlg() {
        return endFlg;
    }
}
