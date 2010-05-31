        import java.util.*;
import java.io.*;
import java.net.*;

import org.imdst.client.ImdstKeyValueClient;
import org.batch.lang.BatchException;

public class TestSockM extends Thread{
    private static boolean startFlg = false;
    private static String[] args = null;
    private int threadNo = 0;
    private long time = 0;
    public static void main(String[] args) {
        try {
            long total = 0;
            TestSockM.args = args;
            Object[] list = new Object[Integer.parseInt(args[2])];
            int threadCount = Integer.parseInt(args[2]);
            for (int i= 0; i < threadCount; i++) {

                TestSockM m = new TestSockM();
                m.threadNo = i;
                m.start();
                list[i] = m;
            }

            Thread.sleep(1000);
            TestSockM.startFlg = true;
            Thread.sleep(6000);
            TestSockM.startFlg = false;
            for (int i= 0; i < list.length; i++) {
System.out.println("adfasf");
System.out.println(startFlg);
                TestSockM m = (TestSockM)list[i];
                m.join();
                total = total + m.time;
            }
            double one = total / threadCount;
            System.out.println(one);
            System.out.println(one / 60);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    public void run() {
        try {
            if (args == null || args.length ==0) {
                System.out.println("args error");
                System.exit(0);
            }

            if (args[0].equals("1")) {
                int counter = 0;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.setValue("datasavekey_" + threadNo, "savedatavaluestr_" + threadNo);
                imdstKeyValueClient.setValue("datasavekey_test" + threadNo, "savedatavaluestr_test" + threadNo);
                imdstKeyValueClient.setValue("datasavekey2_" + threadNo, "savedatavaluestr2_" + threadNo);
                imdstKeyValueClient.setValue("datasavekey2_test" + threadNo, "savedatavaluestr2_test" + threadNo);

                while(true){
                    while (TestSockM.startFlg == true) {
                        imdstKeyValueClient.setValue("datasavekey_" + threadNo + "_" + new Integer(counter).toString(), "savedatavaluestr_" + threadNo + "_" + new Integer(counter).toString());
                        counter++;
                    }
                    if (counter > 0) break;
                }
                time = counter;


                imdstKeyValueClient.close();
            } else if (args[0].equals("2")) {
                int counter = 0;

                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);

                imdstKeyValueClient.autoConnect();
                imdstKeyValueClient.getValue("datasavekey_" + threadNo);
                imdstKeyValueClient.getValue("datasavekey_test" + threadNo);
                imdstKeyValueClient.getValue("datasavekey2_" + threadNo);
                imdstKeyValueClient.getValue("datasavekey2_test" + threadNo);


                while(true) {

                    while (TestSockM.startFlg) {
                        imdstKeyValueClient.getValue("datasavekey_" + threadNo + "_" + new Integer(counter).toString());
                        counter++;

                    }
                    if (counter > 0) break;
                }

                time = counter;
                imdstKeyValueClient.close();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}