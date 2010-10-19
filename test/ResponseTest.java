package test;
import java.util.*;
import java.io.*;
import java.net.*;

import okuyama.imdst.client.ImdstKeyValueClient;
import okuyama.base.lang.BatchException;


public class ResponseTest {

    public static volatile String[] args = null;
    public static volatile boolean startFlg = false;

    public static void main(String[] args) {
        ResponseTest.args = args;
        ResponseTest me = new ResponseTest();
        me.exec(args);
    }


   /**
    * 0=実行指定(1=set 2=get).<br>
    * 1=MasterServers(IP:PORT,IP:PORT).<br>
	* 2=プレフィックス
    * 3=同時スレッド数
    * 4=1スレッドの割り当て数(set、getの上限値)
    * 5=実行時間(秒)
    * 6=Radom=(y or n)
    *
	* @param
	* @return 
	* @throw
	*/
    public void exec (String[] args) {
        try {

			
			if (args.length != 7) {
			    System.out.println("0=実行指定(1=set 2=get)");
			    System.out.println("1=MasterServers(IP:PORT,IP:PORT)");
				System.out.println("2=プレフィックス");
			    System.out.println("3=同時スレッド数");
			    System.out.println("4=1スレッドの割り当て数(set、getの上限値)");
			    System.out.println("5=実行時間(秒)(5秒区切り)");
			    System.out.println("6=Radom=(true or false)");

				System.exit(1);
			}


			String execMethod = args[0];
			String servers = args[1];
			String prefix = args[2];
			int threadCount = Integer.parseInt(args[3]);
			long endCounter = Long.parseLong(args[4]);
			long executeTime = Long.parseLong(args[5]);
			boolean rndFlg = Boolean.parseBoolean(args[6]);

			ResponseTestThread[] threads = new ResponseTestThread[threadCount];

            for (int i= 0; i < threadCount; i++) {

				threads[i] = new ResponseTestThread(i, prefix, rndFlg, endCounter);
            }


            for (int i= 0; i < threads.length; i++) {

				threads[i].start();
            }

			long start = System.nanoTime();
            startFlg = true;

            System.out.println("  ------- Start -------");

			if (execMethod.equals("1")) {
				if (rndFlg) {
	                for(int i = 0; i < (executeTime / 5); i++) {
	                    Thread.sleep(4980);
	                    System.out.println("-- " + ((i + 1) * 5) + "秒");
	                    for (int ii= 0; ii < threads.length; ii++) {
	                        System.out.println(threads[ii].getExecCounter());
	                    }
	                } 
				} else {
	                boolean execFlg = true;
					int inCnt = 0;

	                while(execFlg) {
	                    execFlg = false;

	                    for (int i= 0; i < threads.length; i++) {
							if ((inCnt % 20) == 0) {

		                        System.out.println(threads[i].getExecCounter());
								inCnt = 0;
							}
	                        if(!threads[i].getEndFlg()) execFlg = true;
	                    }
						if (!execFlg) break;
	                    Thread.sleep(250);
						inCnt++;
	                }
				}
			} else if (execMethod.equals("2")) {

				if (rndFlg) {
	                for(int i = 0; i < (executeTime / 5); i++) {
	                    Thread.sleep(4980);
	                    System.out.println("-- " + ((i + 1) * 5) + "秒");
	                    for (int ii= 0; ii < threads.length; ii++) {
	                        System.out.println(threads[ii].getExecCounter());
	                    }
	                } 
				} else {

	                boolean execFlg = true;
					int inCnt = 0;

	                while(execFlg) {
	                    execFlg = false;
	                    for (int i= 0; i < threads.length; i++) {
							if ((inCnt % 20) == 0) {
		                        System.out.println(threads[i].getExecCounter());
								inCnt = 0;
							}
	                        if(!threads[i].getEndFlg()) execFlg = true;
	                    }

						if (!execFlg) break;
	                    Thread.sleep(250);
						inCnt++;
	                }
				}
			}

            startFlg = false;
			long end = System.nanoTime();

            Thread.sleep(500);
            System.out.println("  -------- End --------");
            System.out.println("");
            Thread.sleep(500);

			long total = 0L;
            for (int i= 0; i < threads.length; i++) {

                System.out.println("ThreadNo." + (i+1) + " = " + threads[i].getExecCounter());
                total = total + threads[i].getExecCounter();
            }


            double one = total / threadCount;
            System.out.println("");
            System.out.println("Total Count = " + total);
            System.out.println("Total Time  = " + ((end - start) / 1000 / 1000) + "Milli Second");
			System.out.println();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}