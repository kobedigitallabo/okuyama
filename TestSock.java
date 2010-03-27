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
                System.out.println("{キー値を自動で繰り返し数分変動させて登録}                        コマンド引数{args[0]=1.1, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録Key値, args[4]=登録Value値}");
                System.out.println("{キー値を自動で繰り返し数分変動させて登録}                        コマンド引数{args[0]=1.2, args[1]=\"マスタノードサーバIP:マスタノードサーバPort番号,スレーブマスタノードサーバIP:スレーブマスタノードサーバPort番号\", args[2]=登録件数}");
                System.out.println("{キー値を自動で繰り返し数分変動させて取得}                        コマンド引数{args[0]=2, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数}");
                System.out.println("{キー値を自動で繰り返し数分変動させて取得}                        コマンド引数{args[0]=2.1, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得したいKey値}");
                System.out.println("{キー値を自動で繰り返し数分変動させて取得}                        コマンド引数{args[0]=2.2, args[1]=\"マスタノードサーバIP:マスタノードサーバPort番号,スレーブマスタノードサーバIP:スレーブマスタノードサーバPort番号\", args[2]=取得回数}");
                System.out.println("{キー値を指定してスクリプト実行し取得}                            コマンド引数{args[0]=2.3, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得Key値, args[4]=実行スクリプトコード}");
                System.out.println("{Tagを4パターンで自動的に変動させてキー値は自動変動で登録}        コマンド引数{args[0]=3, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録件数}");
                System.out.println("{指定したTagで関連するキー値を指定回数取得}                       コマンド引数{args[0]=4, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数, args[4]=指定Tag値 (tag1 or tag2 or tag3 or tag4)}");
                System.out.println("{指定したファイルをバイナリデータとして指定したキー値で保存する}  コマンド引数{args[0]=5, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録回数, args[4]=ファイルパス, args[5]=キー値}");
                System.out.println("{指定したキー値でバイナリデータを取得してファイル化する}          コマンド引数{args[0]=6, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数, args[4]=作成ファイルパス, args[5]=キー値}");
                System.out.println("{キー値を自動で繰り返し数分変動させて削除}                        コマンド引数{args[0]=7, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=削除回数}");
                System.out.println("{キー値を指定してデータを削除}                                    コマンド引数{args[0]=8, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=削除したいKey値}");
                System.exit(0);
            }

            if (args[0].equals("1")) {

                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(args[1], port);


                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    // データ登録

                    if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    //if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("ImdstKeyValueClient - error");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } if (args[0].equals("1.1")) {
                
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                
                // マスタサーバに接続
                imdstKeyValueClient.connect(args[1], port);


                long start = new Date().getTime();
                if (!imdstKeyValueClient.setValue(args[3], args[4])) {
                //if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("ImdstKeyValueClient - error");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();

            } else if (args[0].equals("1.2")) {
                // AutoConnectionモード
                // クライアントインスタンスを作成
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);
                imdstKeyValueClient.autoConnect();

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[2]);i++) {
                    // データ登録
                    if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("ImdstKeyValueClient - error");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("2")) {
                
                int port = Integer.parseInt(args[2]);
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
                
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを取得(Keyのみ)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                ret = imdstKeyValueClient.getValue(args[3]);
                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println(ret[1]);
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("2.11")) {

                int port = Integer.parseInt(args[2]);
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
            }else if (args[0].equals("2.2")) {
                // AutoConnectionモード
                // ImdstKeyValueClientを使用してデータを取得(Keyのみ)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                String[] infos = args[1].split(",");
                imdstKeyValueClient.setConnectionInfos(infos);
                imdstKeyValueClient.autoConnect();

                String[] ret = null;

                long start = new Date().getTime();
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
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("2.3")) {
                
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを取得(Keyのみ)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                ret = imdstKeyValueClient.getValueScript(args[3], args[4]);
                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println(ret[1]);
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            } else if (args[0].equals("3")) {

                int port = Integer.parseInt(args[2]);
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
            } else if (args[0].equals("3.1")) {
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを保存(Tagあり)
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] setTag = args[4].split(" ");

                int counter = 0;
                String keyStr = null;
                imdstKeyValueClient.setValue(args[3], setTag, args[5]);

                imdstKeyValueClient.close();
            } else if (args[0].equals("4")) {
                int port = Integer.parseInt(args[2]);
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
                        /*for (int idx = 0; idx < keys.length; idx++) {
                            System.out.println(keys[idx]);
                        }*/
                    } else if (ret[0].equals("false")) {
                        System.out.println("データなし");
                    } else if (ret[0].equals("error")) {
                        System.out.println(ret[1]);
                    }
                }

                if (keys != null) {
                    for (int ii = 0; ii < keys.length; ii++) {
                        System.out.println("Key=[" + keys[ii] + "]");
                        String[] ret = imdstKeyValueClient.getValue(keys[ii]);
                        System.out.println("Value=[" + ret[1] + "]");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start));
                imdstKeyValueClient.close();

            } else if (args[0].equals("5")) {
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientでファイルをキー値で保存する
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
                int port = Integer.parseInt(args[2]);
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
            } else if (args[0].equals("7")) {
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを削除
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    ret = imdstKeyValueClient.removeValue("datasavekey_" + new Integer(i).toString());
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
            } else if (args[0].equals("8")) {
                int port = Integer.parseInt(args[2]);
                // ImdstKeyValueClientを使用してデータを削除
                ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();

                ret = imdstKeyValueClient.removeValue(args[3]);
                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println(ret[1]);
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                imdstKeyValueClient.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}