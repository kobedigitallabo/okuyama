package test;
import java.util.*;
import java.io.*;
import java.net.*;

import org.imdst.client.ImdstKeyValueClient;
import org.batch.lang.BatchException;
import com.danga.MemCached.*;


public class TestSockMem {

    public static volatile String[] args = null;
    public static volatile boolean startFlg = false;

    public static void main(String[] args) {
        TestSockMem.args = args;
        TestSockMem me = new TestSockMem();
        me.exec(args);
    }

    // 引数は
    // 実行タイプ 1:登録 2:取得
    // IP:Port
    // 同時スレッド数
    public void exec (String[] args) {
        try {
            long total = 0;
            Object  [] list = new Object[Integer.parseInt(args[1])];
            int threadCount = Integer.parseInt(args[1]);
            TestMem m = null;
            int count = 0;
            for (int i= 0; i < threadCount; i++) {
                if (args[0].equals("4")) {
                    m = new TestMem();
                    m.setMaxPrefix(count);
                    count = count + 100000;
                } else {
                    m = new TestMem();
                    if (args.length > 2) 
                        m.setMaxPrefix(Integer.parseInt(args[2]));
                }
                m.setThreadNo(i);
                list[i] = m;
            }


            for (int i= 0; i < list.length; i++) {

                m = (TestMem)list[i];
                m.start();
                list[i] = m;
            }

            String[] serverlist = { "192.168.2.116:11211","192.168.2.185:11211"};

            // initialize the pool for memcache servers
            SockIOPool pool = SockIOPool.getInstance();
            pool.setServers( serverlist );

            pool.initialize();

            startFlg = true;

            System.out.println("  ------- Start -------");
            if (args[0].equals("1")) {
                String pre = "  --";
                for(int i = 1; i < 7; i++) {
                    Thread.sleep(9990);
                    System.out.println(pre + " " + (i * 10) + "秒");
                    pre = pre + "--";
                }

            }else if (args[0].equals("2")) {
                String pre = "  --";
                for(int i = 1; i < 7; i++) {
                    Thread.sleep(9990);
                    System.out.println(pre + " " + (i * 10) + "秒");
                    pre = pre + "--";
                }
            } else if (args[0].equals("3")) {
                Thread.sleep(120000);
            } else if (args[0].equals("4")) {
                boolean execFlg = true;
                while(execFlg) {
                    execFlg = false;
                    for (int i= 0; i < list.length; i++) {
                        m = (TestMem)list[i];
                        System.out.println(m.getExecCounter());
                        if(m.getExecCounter() != 100000) execFlg = true;
                    }
                    Thread.sleep(5000);
                    
                }
                
            }
            startFlg = false;

            Thread.sleep(500);
            System.out.println("  -------- End --------");
            System.out.println("");
            Thread.sleep(500);
            for (int i= 0; i < list.length; i++) {

                m = (TestMem)list[i];
                System.out.println("ThreadNo." + (i+1) + " = " + m.getExecCounter() + "件");
                total = total + m.getExecCounter();
            }


            double one = total / threadCount;
            System.out.println("");
            System.out.println("合計処理件数 = " + total + "件");
            //System.out.println("1 Thread Avg Query Count = " + one);
            //System.out.println("QPS = " + (total / 60));
            System.exit(1);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}