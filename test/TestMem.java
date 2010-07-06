package test;

import java.util.*;
import java.io.*;
import java.net.*;


import com.danga.MemCached.*;

public class TestMem extends Thread {

    public volatile int threadNo = 0;
    public volatile long execCounter = 0;

    private volatile String prefix = "";
    private volatile int maxPrefix = 0;

    public void run() {
        while(!TestSockMem.startFlg){}
        try {

            if (TestSockMem.args[0].equals("1")) {
                // "DataSaveKey_threadNo_0～DataSaveKeyいけるとこまで"登録
                int counter = 1;


                MemCachedClient mc = new MemCachedClient();
                mc.setCompressEnable( false );
                mc.setPrimitiveAsString( true );
                mc.set( new Integer(999999).toString(), new Integer(999999).toString());
                mc.get( new Integer(999999).toString());

                Random rnd = new Random();

                String key = "DataSaveKey_" + threadNo +  "_";
                String value= "Value012345678901234567890123456789_" + threadNo + "_";
                while(true &&  TestSockMem.startFlg){

                    //if(!mc.set(key + this.execCounter, value + this.execCounter)) {
                    if(!mc.set(key + rnd.nextInt(1000000), value)) {
                        System.out.println("Error");
                    }
                    this.execCounter++;
                }

            } else if (TestSockMem.args[0].equals("2")) {

                // "DataSaveKey0～指定した値まで"でランダムに取得(標準出力なし)
                int counter = 1;

                MemCachedClient mc = new MemCachedClient();
                mc.setCompressEnable( false );
                mc.setPrimitiveAsString( true );
                mc.set( new Integer(999999).toString(), new Integer(999999).toString());
                mc.get( new Integer(999999).toString());

                String key = "DataSaveKey";
                //Randomクラスのインスタンス化
                Random rnd = new Random();


                while(true &&  TestSockMem.startFlg){
                    Object ret = mc.get(key + rnd.nextInt(maxPrefix));
                    if (ret == null) {
                        System.out.println("Data Not Found");
                    }

                    //if (this.execCounter % 1000 == 0) System.out.println(this.execCounter);
                    this.execCounter++;
                }

            } else if (TestSockMem.args[0].equals("3")) {

                // "DataSaveKey0～指定した値まで"でランダムに取得(標準出力あり)
                int counter = 1;

                MemCachedClient mc = new MemCachedClient();
                mc.setCompressEnable( false );
                mc.setPrimitiveAsString( true );
                mc.set( new Integer(999999).toString(), new Integer(999999).toString());
                mc.get( new Integer(999999).toString());

                String key = "DataSaveKey";
                //Randomクラスのインスタンス化
                Random rnd = new Random();


                while(true &&  TestSockMem.startFlg){
                    Object ret = mc.get(key + rnd.nextInt(maxPrefix));
                    if (ret != null) {
                        System.out.println(ret);
                    } else {
                        System.out.println("Data Not Found");
                    }
                }
            } else if (TestSockMem.args[0].equals("4")) {

                // "DataSaveKey0～1スレッド100000件"まで登録
                int counter = 1;


                MemCachedClient mc = new MemCachedClient();
                mc.setCompressEnable( false );
                mc.setPrimitiveAsString( true );
                mc.set( new Integer(999999).toString(), new Integer(999999).toString());
                mc.get( new Integer(999999).toString());


                String key = "DataSaveKey";
                String value= "Value012345678901234567890123456789";
                for (int i = this.maxPrefix; i < (this.maxPrefix + 100000); i++){

                    if(!mc.set(key + i, value + i)) {
                        System.out.println("Error");
                    }
                    this.execCounter++;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void setThreadNo(int no) {
        threadNo = no;
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

}
