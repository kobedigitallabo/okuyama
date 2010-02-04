import java.util.*;
import java.io.*;
import java.net.*;

import org.imdst.client.ImdstKeyValueClient;
import org.batch.lang.BatchException;

public class TestSock {
    public static void main(String[] args) {
        try {
            if (args == null || args.length ==0) {

                System.out.println("{キー値を自動で繰り返し数分変動させて登録}                        コマンド引数{args[0]=1, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録件数}");
                System.out.println("{キー値を自動で繰り返し数分変動させて取得}                        コマンド引数{args[0]=2, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数}");
                System.out.println("{Tagを4パターンで自動的に変動させてキー値は自動変動で登録}        コマンド引数{args[0]=3, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録件数}");
                System.out.println("{指定したTagで関連するキー値を指定回数取得}                       コマンド引数{args[0]=4, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数, args[4]=指定Tag値 (tag1 or tag2 or tag3 or tag4)}");
                System.out.println("{指定したファイルをバイナリデータとして指定したキー値で保存する}  コマンド引数{args[0]=5, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録回数, args[4]=ファイルパス, args[5]=キー値}");
                System.out.println("{指定したキー値でバイナリデータを取得してファイル化する}          コマンド引数{args[0]=6, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数, args[4]=作成ファイルパス, args[5]=キー値}");
                System.exit(0);
            }
            int port = Integer.parseInt(args[2]);

            if (args[0].equals("1")) {

                // ImdstKeyValueClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(args[1], port);

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    // データ登録
                    if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("ImdstKeyValueClient - error");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("2")) {

                // ImdstKeyValueClientを使用してデータを取得(Keyのみ)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
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
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("2.1")) {

                // ImdstKeyValueClientを使用してデータを取得(Keyのみ)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
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
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();

            } else if (args[0].equals("3")) {

                // ImdstKeyValueClientを使用してデータを保存(Tagあり)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] tag1 = {"tag1"};
                String[] tag2 = {"tag1","tag2"};
                String[] tag3 = {"tag1","tag2","tag3"};
                String[] tag4 = {"tag4"};
                String[] setTag = null;
                int counter = 0;

                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    if (counter == 0) {
                        setTag = tag1;
                        counter++;
                    } else if (counter == 1) {
                        setTag = tag2;
                        counter++;
                    } else if (counter == 2) {
                        setTag = tag3;
                        counter++;
                    } else if (counter == 3) {
                        setTag = tag4;
                        counter = 0;
                    }

                    if (!imdstKeyValueClient.setValue("tagsampledatakey_" + new Integer(i).toString(), setTag, "tagsamplesavedata_" + new Integer(i).toString())) {
                        System.out.println("ImdstKeyValueClient - error");
                    }
                }

                imdstKeyValueClient.close();
            } else if (args[0].equals("4")) {

                // ImdstKeyValueClientを使用してデータを取得(Tagでの取得)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] keys = null;
                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                    Object[] ret = imdstKeyValueClient.getTagKeys(args[4]);
                    if (ret[0].equals("true")) {
                        // データ有り
                        keys = (String[])ret[1];
                        /*for (int i = 0; i < keys.length; i++) {
                            System.out.println(keys[i]);
                        }*/
                    } else if (ret[0].equals("false")) {
                        System.out.println("データなし");
                    } else if (ret[0].equals("error")) {
                        System.out.println(ret[1]);
                    }
                }

                if (keys != null) {
                    for (int ii = 0; ii < keys.length; ii++) {
                        System.out.println(keys[ii]);
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start));
                imdstKeyValueClient.close();

            } else if (args[0].equals("5")) {

                // ImdstKeyValueClientをファイルをキー値で保存する
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] keys = null;
                long start = new Date().getTime();
                // args[4]はファイル名、args[5]はキー値
                for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                    // ファイルをバイナリで読み込み
                    byte[] fileByte = null;
                    File file = new File(args[4]);
                    fileByte = new byte[new Long(file.length()).intValue()];
                    FileInputStream fis = new FileInputStream(file);
                    fis.read(fileByte, 0, fileByte.length);
                    if (!imdstKeyValueClient.setByteValue(args[5], fileByte)) {
                        System.out.println("ImdstKeyValueClient - error");
                    }
                    fis.close();
                }
                long end = new Date().getTime();
                System.out.println((end - start));
                imdstKeyValueClient.close();

            } else if (args[0].equals("6")) {

                // ImdstKeyValueClientを使用してデータを取得(Keyのみ)(バイナリ)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                Object[] ret = null;
                long start = new Date().getTime();
                
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    ret = imdstKeyValueClient.getByteValue(args[5]);
                    if (ret[0].equals("true")) {
                        // データ有り
                        byte[] fileByte = null;
                        File file = new File(args[4]);
                        FileOutputStream fos = new FileOutputStream(file);
                        fileByte = (byte[])ret[1];
                        fos.write(fileByte, 0, fileByte.length);
                        fos.close();
                    } else if (ret[0].equals("false")) {
                        System.out.println("データなし");
                    } else if (ret[0].equals("error")) {
                        System.out.println(ret[1]);
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start));
                imdstKeyValueClient.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}