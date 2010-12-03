package test;

import java.util.*;
import java.io.*;
import java.net.*;

import com.danga.MemCached.*;


public class ResponseTestThreadMem extends Thread {

    public volatile int threadNo = 0;
    public volatile String prefix = "";

    public volatile long execCounter = 0;
    public volatile boolean rndFlg = false;

    public volatile long endCounter = 0;

    private volatile boolean endFlg = false;


    private static final String tmpKey = "DataSaveKey910111213";
    private static final String tmpValue = "DataSaveValue123456789101112131415161718192021222324252627282930313233343536373839404142quyrt_";



    public ResponseTestThreadMem(int threadNo, String prefix, boolean rndFlg, long endCounter) {
        this.threadNo = threadNo;
        this.prefix = prefix;
        this.rndFlg = rndFlg;
        this.endCounter = endCounter;
    }


    public void run() {

        while(!ResponseTestMem.startFlg){}
        try {
            if (ResponseTestMem.args[0].equals("1")) {


                MemCachedClient memcachedClient = new MemCachedClient();
                memcachedClient.setCompressEnable( false );
                memcachedClient.setPrimitiveAsString( true );

                String[] infos = ResponseTestMem.args[1].split(",");

                String key = tmpKey + threadNo + "_" + prefix + "_";
                String value= tmpValue + threadNo + "_" + prefix + "_";


                if (this.rndFlg) {

                    // Random
                    Random rnd = new Random();
                    int rndVal = new Long(this.endCounter).intValue();
                    while(true &&  ResponseTestMem.startFlg){

                        int appendInt = rnd.nextInt(rndVal);
                        if(!memcachedClient.set(key + appendInt, value + appendInt)) {
                            System.out.println("Error");
                        }
                        this.execCounter++;
                    }
                } else {

                    // Loop
                    while(true &&  ResponseTestMem.startFlg){

                        if(!memcachedClient.set(key + this.execCounter, value + this.execCounter)) {
                            System.out.println("Error");
                        }
                        this.execCounter++;
                        if (this.endCounter <= this.execCounter) break;
                    }
                }
            } else if (ResponseTestMem.args[0].equals("2")) {


                MemCachedClient memcachedClient = new MemCachedClient();
                memcachedClient.setCompressEnable( false );
                memcachedClient.setPrimitiveAsString( true );


                String[] infos = ResponseTestMem.args[1].split(",");


                String key = tmpKey + threadNo + "_" + prefix + "_";
                String[] ret = null;

                if (rndFlg) {

                    // Random
                    Random rnd = new Random();
                    int rndVal = new Long(this.endCounter).intValue();

                    while(true &&  ResponseTestMem.startFlg){

                        Object ret2 = memcachedClient.get(key + rnd.nextInt(rndVal));
                        if (ret2 != null) {
                            //System.out.println(ret2);
                        } else {
                            System.out.println("Data Not Found");
                        }

                        this.execCounter++;
                    }
                } else {

                    // Loop
                    while(true &&  ResponseTestMem.startFlg){

                        Object ret2 = memcachedClient.get(key + this.execCounter);
                        if (ret2 != null) {
                            //System.out.println(ret2);
                        } else {
                            System.out.println("Data Not Found");
                        }


                        this.execCounter++;
                        if (this.endCounter <= this.execCounter) break;
                    }
                }
            }
            this.endFlg = true;
        } catch (Exception e) {
            e.printStackTrace();
            endFlg = true;
        }
    }


    public long getExecCounter() {
        return this.execCounter;
    }

    public boolean getEndFlg() {
        return this.endFlg;
    }
}
