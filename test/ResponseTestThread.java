package test;

import java.util.*;
import java.io.*;
import java.net.*;

import okuyama.imdst.client.ImdstKeyValueClient;
import okuyama.base.lang.BatchException;

public class ResponseTestThread extends Thread {

    public volatile int threadNo = 0;
    public volatile String prefix = "";

    public volatile long execCounter = 0;
	public volatile boolean rndFlg = false;

    public volatile long endCounter = 0;

    private volatile boolean endFlg = false;


	private static String tmpKey = "DataSaveKey123456789101112131415161718192021222324252627282930313233343536373839404142lkio_";
	private static String tmpValue = "DataSaveValue123456789101112131415161718192021222324252627282930313233343536373839404142quyrt_";



	public ResponseTestThread(int threadNo, String prefix, boolean rndFlg, long endCounter) {
		this.threadNo = threadNo;
		this.prefix = prefix;
        this.rndFlg = rndFlg;
		this.endCounter = endCounter;
	}


    public void run() {
		ImdstKeyValueClient imdstKeyValueClient = null;
        while(!ResponseTest.startFlg){}
        try {
            if (ResponseTest.args[0].equals("1")) {

                imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = ResponseTest.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();

                String key = tmpKey + threadNo + "_" + prefix + "_";
                String value= tmpValue + threadNo + "_" + prefix + "_";



				if (rndFlg) {

					// Random
	                Random rnd = new Random();
					int rndVal = new Long(this.endCounter).intValue();
	                while(true &&  ResponseTest.startFlg){

						int appendInt = rnd.nextInt(rndVal);
	                    if(!imdstKeyValueClient.setValue(key + appendInt, value + appendInt)) {
	                        System.out.println("Error");
	                    }
	                    this.execCounter++;
	                }
				} else {

					// Loop
	                while(true &&  ResponseTest.startFlg){

	                    if(!imdstKeyValueClient.setValue(key + this.execCounter, value + this.execCounter)) {
	                        System.out.println("Error");
	                    }
	                    this.execCounter++;
						if (this.endCounter <= this.execCounter) break;
	                }
				}
            } else if (ResponseTest.args[0].equals("2")) {

                imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = ResponseTest.args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();

                String key = tmpKey + threadNo + "_" + prefix + "_";
				String[] ret = null;

				if (rndFlg) {

					// Random
	                Random rnd = new Random();
					int rndVal = new Long(this.endCounter).intValue();

	                while(true &&  ResponseTest.startFlg){

						ret = imdstKeyValueClient.getValue(key + rnd.nextInt(rndVal));

	                    if(ret[0].length() == 5) {
	                        System.out.println("Not Found");
	                    }
	                    this.execCounter++;
	                }
				} else {

					// Loop
	                while(true &&  ResponseTest.startFlg){

						ret = imdstKeyValueClient.getValue(key + this.execCounter);

	                    if(ret[0].length() == 5) {
	                        System.out.println("Not Found");
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
        } finally {
			try {
				imdstKeyValueClient.close();
			} catch (Exception e2) {
			}
		}
    }


    public long getExecCounter() {
        return this.execCounter;
    }

    public boolean getEndFlg() {
        return this.endFlg;
    }
}
