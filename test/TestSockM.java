package test;
import java.util.*;
import java.io.*;
import java.net.*;

import org.okuyama.imdst.client.ImdstKeyValueClient;
import org.okuyama.base.lang.BatchException;


public class TestSockM {

    public static String[] args = null;

    public static void main(String[] args) {
        TestSockM.args = args;
        TestSockM me = new TestSockM();
        me.exec(args);
    }

    // 引数は
    // 実行タイプ 1:登録 2:取得
    // IP:Port
    // 同時スレッド数
    public void exec (String[] args) {
        try {
            long total = 0;
            Object  [] list = new Object[Integer.parseInt(args[2])];
            int threadCount = Integer.parseInt(args[2]);
            MTest m = null;
            for (int i= 0; i < threadCount; i++) {

                m = new MTest();
                m.setPrefix(args[3]);
                m.threadNo = i;
                list[i] = m;
            }


            for (int i= 0; i < list.length; i++) {

                m = (MTest)list[i];
                m.start();
                list[i] = m;
            }
            if (args[0].equals("1") || args[0].equals("2")) {
                Thread.sleep(59998);
            } else if (args[0].equals("3")) {
                Thread.sleep(120000);
            }
            System.out.println("end");
            for (int i= 0; i < list.length; i++) {

                m = (MTest)list[i];
                total = total + m.getExecCounter();
            }


            double one = total / threadCount;

            System.out.println("Total Query Count = " + total);
            System.out.println("1 Thread Avg Query Count = " + one);
            System.out.println("QPS = " + (total / 60));
            System.exit(1);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}