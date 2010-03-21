import java.util.*;
import java.io.*;
import java.net.*;

import org.imdst.client.ImdstKeyValueClient;
import org.batch.lang.BatchException;

public class TestSockM extends Thread{
    private static String[] args = null;
    private long time = 0;
    public static void main(String[] args) {
        try {
            long total = 0;
            TestSockM.args = args;
            ArrayList list = new ArrayList();
            int threadCount = Integer.parseInt(args[3]);
            for (int i= 0; i < threadCount; i++) {
           
                TestSockM m = new TestSockM();
                m.start();
                list.add(m);
            }
            for (int i= 0; i < list.size(); i++) {

                TestSockM m = (TestSockM)list.get(i);
                m.join();
                total = total + m.time;
            }
            double one = total / threadCount;
            System.out.println(one);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    public void run() {
        try {
            if (args == null || args.length ==0) {
                System.out.println("エラー");
                System.exit(0);
            }
            int port = Integer.parseInt(args[2]);

            if (args[0].equals("1")) {
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();


                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[2]);i++) {
                    if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("ImdstKeyValueClient - error");
                    }
                }
                long end = new Date().getTime();
                time = (end - start);
                //System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("2")) {

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                long start = new Date().getTime();
                String[] ret = null;

                for (int i = 0; i < Integer.parseInt(args[2]);i++) {
                    ret = imdstKeyValueClient.getValue("datasavekey_" + new Integer(i).toString());
                    if (ret[0].equals("true")) {
                        // データ有り
                    } else if (ret[0].equals("false")) {
                        System.out.println("データなし");
                    } else if (ret[0].equals("error")) {
                        System.out.println(ret[1]);
                    }
                }
                long end = new Date().getTime();
                time = (end - start);
                //System.out.println((end - start) + "milli second");
                imdstKeyValueClient.close();

            } else if (args[0].equals("2.1")) {

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);
                imdstKeyValueClient.autoConnect();

                long start = new Date().getTime();
                String[] ret = null;

                for (int i = 0; i < Integer.parseInt(args[2]);i++) {
                    ret = imdstKeyValueClient.getValue("datasavekey_" + new Integer(i).toString());
                    if (ret[0].equals("true")) {
                        // データ有り
                        System.out.println(ret[1]);
                    } else if (ret[0].equals("false")) {
                        System.out.println("データなし");
                    } else if (ret[0].equals("error")) {
                        System.out.println(ret[1]);
                    }
                }
                long end = new Date().getTime();
                time = (end - start);
                //System.out.println((end - start) + "milli second");
                imdstKeyValueClient.close();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
