import java.util.*;
import java.io.*;
import java.net.*;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.base.lang.BatchException;

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
                System.out.println("{キー値を自動で繰り返し数分変動させて取得}                        コマンド引数{args[0]=2.22, args[1]=\"マスタノードサーバIP:マスタノードサーバPort番号,スレーブマスタノードサーバIP:スレーブマスタノードサーバPort番号\", args[2]=Key値}");
                System.out.println("{キー値を指定してスクリプト実行し取得}                            コマンド引数{args[0]=2.3, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得Key値, args[4]=実行スクリプトコード}");
                System.out.println("{Tagを4パターンで自動的に変動させてキー値は自動変動で登録}        コマンド引数{args[0]=3, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録件数}");
                System.out.println("{指定したTagで関連するキー値を指定回数取得}                       コマンド引数{args[0]=4, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数, args[4]=指定Tag値 (tag1 or tag2 or tag3 or tag4)}, args[5]=Key値として存在しない値の取得有無 true=tag値として登録された過去があればkey値は返す false=Key値が存在しなければ返さない");
                System.out.println("{指定したファイルをバイナリデータとして指定したキー値で保存する}  コマンド引数{args[0]=5, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=登録回数, args[4]=ファイルパス, args[5]=キー値}");
                System.out.println("{指定したキー値でバイナリデータを取得してファイル化する}          コマンド引数{args[0]=6, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=取得回数, args[4]=作成ファイルパス, args[5]=キー値}");
                System.out.println("{キー値を自動で繰り返し数分変動させて削除}                        コマンド引数{args[0]=7, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=削除回数}");
                System.out.println("{キー値を指定してデータを削除}                                    コマンド引数{args[0]=8, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号, args[3]=削除したいKey値}");
                System.out.println("{トランザクションを開始する}                                      コマンド引数{args[0]=9, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号}");
                System.out.println("{Transactionを開始してデータをLock後、データを更新、取得し、Lockを解除}  コマンド引数{args[0]=10, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号. args[3]=Key値, args[4]=Lock取得維持時間, args[5]=Lock取得待ち時間}");
                System.out.println("{一度登録した値はエラーとなる}                                    コマンド引数{args[0]=11, args[1]=マスタノードサーバIP, args[2]=マスタノードサーバPort番号. args[3]=Key値, args[4]=Value値}");
                System.exit(0);
            }


            if (args[0].equals("1")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);

                Random rnd = new Random();
                StringBuilder strBuf =null; 
                if (args.length > 4) {
                    strBuf = new StringBuilder(6000*10);
                    for (int i = 0; i < 3000; i++) {
                        strBuf.append(rnd.nextInt(1999999999));
                    }
                }

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    // データ登録
                    if (args.length > 4) {

                        //if (!okuyamaClient.setValue("datasavekey_" + args[4] + "_" + new Integer(i).toString(), "savedatavaluestr_" + args[4] + "_" + new Integer(i).toString())) {
                        if (!okuyamaClient.setValue("datasavekey_" + args[4] + "_" + new Integer(i).toString(), "savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol<MKIUJNBGTRFBVFREDCXSWQAZXSWEDCVFRTGBNHY678745_savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol<MKIUJNBGTRFBVFREDCXSWQAZXSWEDCVFRTGBNHY678745savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol" + args[4] + "_" + new Integer(i).toString())) {
                        //if (!okuyamaClient.setValue("datasavekey_" + args[4] + "_" + new Integer(i).toString(), "savedatavaluestr0987654321" + strBuf.toString() + "_" + args[4] + "_" + new Integer(i).toString())) {
                            System.out.println("OkuyamaClient - error");
                        } else {
                            System.out.println("Store[" + "datasavekey_" + args[4] + "_" + new Integer(i).toString() + "]");
                        }
                    } else {
                        if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                        //if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                            System.out.println("OkuyamaClient - error");
                        }
                    }
                    //if ((i % 1000) == 0) System.out.println(i);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } if (args[0].equals("1.1")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                if (!okuyamaClient.setValue(args[3], args[4])) {
                //if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("OkuyamaClient - error");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();

            } else if (args[0].equals("1.2")) {
                // AutoConnectionモード
                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                String[] infos = args[1].split(",");
                okuyamaClient.setConnectionInfos(infos);
                okuyamaClient.autoConnect();

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[2]);i++) {
                    // データ登録
                    if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("OkuyamaClient - error");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } if (args[0].equals("1.3")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);

                StringBuffer bufs = new StringBuffer();

                int idx = 0;

                for (int i = 0; i < Integer.parseInt(args[4]); i++) {
                    if (idx == 0) {
                        bufs.append("a");
                        idx++;
                    } else if (idx == 1) {
                        bufs.append("b");
                        idx++;
                    } else if (idx == 2) {
                        bufs.append("c");
                        idx++;
                    } else if (idx == 3) {
                        bufs.append("d");
                        idx++;
                    } else if (idx == 4) {
                        bufs.append("e");
                        idx = 0;
                    }
                }

                    
                long start = new Date().getTime();

                if (!okuyamaClient.setValue(args[3], bufs.toString())) {
                //if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("OkuyamaClient - error");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("2")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                if (args.length > 4) {
                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        ret = okuyamaClient.getValue("datasavekey_" + args[4] + "_" + new Integer(i).toString());
                        if (ret[0].equals("true")) {
                            // データ有り
                            System.out.println(ret[1]);
                        } else if (ret[0].equals("false")) {
                            System.out.println("データなし");
                        } else if (ret[0].equals("error")) {
                            System.out.println(ret[1]);
                        }
                    }
                } else {
                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        ret = okuyamaClient.getValue("datasavekey_" + new Integer(i).toString());
                        if (ret[0].equals("true")) {
                            // データ有り
                            System.out.println(ret[1]);
                        } else if (ret[0].equals("false")) {
                            System.out.println("データなし");
                        } else if (ret[0].equals("error")) {
                            System.out.println(ret[1]);
                        }
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("2.1")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                ret = okuyamaClient.getValue(args[3]);
                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println("Value=[" + ret[1] + "]");
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("2.11")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    ret = okuyamaClient.getValue("datasavekey_" + new Integer(i).toString());
                    if (ret[0].equals("true")) {
                        // データ有り
                    } else if (ret[0].equals("false")) {
                        System.out.println("datasavekey_" + new Integer(i).toString() + " = データなし");
                    } else if (ret[0].equals("error")) {
                        System.out.println(ret[1]);
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("2.2")) {
                // AutoConnectionモード
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                String[] infos = args[1].split(",");
                okuyamaClient.setConnectionInfos(infos);
                okuyamaClient.autoConnect();

                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[2]);i++) {
                    ret = okuyamaClient.getValue("datasavekey_" + new Integer(i).toString());
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

                okuyamaClient.close();
            } else if (args[0].equals("2.22")) {
                // AutoConnectionモード
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                String[] infos = args[1].split(",");
                okuyamaClient.setConnectionInfos(infos);
                okuyamaClient.autoConnect();

                String[] ret = null;

                long start = new Date().getTime();
                ret = okuyamaClient.getValue(args[2]);
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

                okuyamaClient.close();

            } else if (args[0].equals("2.3")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                ret = okuyamaClient.getValueScript(args[3], args[4]);
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

                okuyamaClient.close();
            } else if (args[0].equals("2.33")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                    ret = okuyamaClient.getValueScript("datasavekey_" + new Integer(i).toString(), "var dataValue; var retValue = ''; var execRet = '0'; if (dataValue.indexOf('99') != -1) {   retValue = dataValue;   execRet = '1';}");
                    //if (ret[0].equals("true")) System.out.println(ret[1]);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("2.4")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                ret = okuyamaClient.getValueScriptForUpdate(args[3], args[4]);
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

                okuyamaClient.close();
            } else if (args[0].equals("3")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagあり)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] tag1 = {"tag1"};
                String[] tag2 = {"tag1","tag2"};
                String[] tag3 = {"tag1","tag2","tag3"};
                String[] tag4 = {"tag4"};
                String[] setTag = null;
                int counter = 0;

                long start = new Date().getTime();

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

                    if (!okuyamaClient.setValue("tagsampledatakey_" + new Integer(i).toString(), setTag, "tagsamplesavedata_" + new Integer(i).toString())) {
                        System.out.println("OkuyamaClient - error");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("3.1")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagあり)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] setTag = args[4].split(" ");

                int counter = 0;
                String keyStr = null;

                long start = new Date().getTime();
                okuyamaClient.setValue(args[3], setTag, args[5]);
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("4")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Tagでの取得)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] keys = null;
                boolean noExistsData = true;
                if (args.length > 5) noExistsData = new Boolean(args[5]).booleanValue();

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                    Object[] ret = okuyamaClient.getTagKeys(args[4], noExistsData);
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
                        String[] ret = okuyamaClient.getValue(keys[ii]);
                        System.out.println("Value=[" + ret[1] + "]");
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start));
                okuyamaClient.close();

            } else if (args[0].equals("5")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientでファイルをキー値で保存する
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
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
                    //okuyamaClient.setCompressMode(true);
                    if (!okuyamaClient.setByteValue(args[5], fileByte)) {
                        System.out.println("OkuyamaClient - error");
                    }
                    fis.close();
                }
                long end = new Date().getTime();
                System.out.println((end - start));
                okuyamaClient.close();
            } else if (args[0].equals("5.1")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientでファイルのバイナリデータをBase64にエンコードして文字列として保存
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] keys = null;
                long start = new Date().getTime();
                // args[3]はファイル名、args[4]はキー値

                // ファイルをバイナリで読み込み
                byte[] fileByte = null;
                File file = new File(args[3]);
                fileByte = new byte[new Long(file.length()).intValue()];
                FileInputStream fis = new FileInputStream(file);
                fis.read(fileByte, 0, fileByte.length);
                //okuyamaClient.setCompressMode(true);
                if (!okuyamaClient.sendByteValue(args[4], fileByte)) {
                    System.out.println("OkuyamaClient - error");
                }
                fis.close();

                long end = new Date().getTime();
                System.out.println((end - start));
                okuyamaClient.close();

            } else if (args[0].equals("6")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)(バイナリ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                Object[] ret = null;
                long start = new Date().getTime();
                
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    //okuyamaClient.setCompressMode(true);
                    ret = okuyamaClient.getByteValue(args[5]);
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
                okuyamaClient.close();
            } else if (args[0].equals("6.1")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)(バイナリ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                Object[] ret = null;
                long start = new Date().getTime();
                
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    //okuyamaClient.setCompressMode(true);
                    ret = okuyamaClient.getByteValueVer2(args[5]);
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
                okuyamaClient.close();
            } else if (args[0].equals("6.2")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)(バイナリ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                Object[] ret = null;
                long start = new Date().getTime();
                
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    //okuyamaClient.setCompressMode(true);
                    ret = okuyamaClient.readByteValue(args[5]);
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
                okuyamaClient.close();
            } else if (args[0].equals("7")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを削除
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                if (args.length > 4) {

                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        ret = okuyamaClient.removeValue("datasavekey_" + args[4] + "_" + new Integer(i).toString());
                        if (ret[0].equals("true")) {
                            // データ有り
                            System.out.println(ret[1]);
                        } else if (ret[0].equals("false")) {
                            System.out.println("データなし");
                        } else if (ret[0].equals("error")) {
                            System.out.println(ret[1]);
                        }
                    }
                } else {

                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        ret = okuyamaClient.removeValue("datasavekey_" + new Integer(i).toString());
                        if (ret[0].equals("true")) {
                            // データ有り
                            System.out.println(ret[1]);
                        } else if (ret[0].equals("false")) {
                            System.out.println("データなし");
                        } else if (ret[0].equals("error")) {
                            System.out.println(ret[1]);
                        }
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("7.1")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してTag用のテストKey値データを削除
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    ret = okuyamaClient.removeValue("tagsampledatakey_" + new Integer(i).toString());
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

                okuyamaClient.close();
            } else if (args[0].equals("8")) {
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを削除
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();

                ret = okuyamaClient.removeValue(args[3]);
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

                okuyamaClient.close();
            } else if (args[0].equals("9")) {
                int port = Integer.parseInt(args[2]);
                // Transactionを開始してデータをLock後、データを更新、取得し、Lockを解除
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();

                okuyamaClient.startTransaction();
                okuyamaClient.lockData("datasavekey_3", 20, 5);
                okuyamaClient.lockData("datasavekey_2", 5, 5);
                if (!okuyamaClient.setValue("datasavekey_3", "locktestdata")) {
                    
                    System.out.println("OkuyamaClient - Lock Update Error");
                }
                ret = okuyamaClient.getValue("datasavekey_3");
                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println(ret[1]);
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }


                //Thread.sleep(10000);
                //okuyamaClient.releaseLockData("datasavekey_3");

                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("10")) {
                int port = Integer.parseInt(args[2]);
                // Transactionを開始してデータをLock後、データを更新、取得し、Lockを解除

                // 引数はLock対象のKey値, Lock維持時間(秒)(0は無制限), Lockが既に取得されている場合の取得リトライし続ける時間(秒)(0は1回取得を試みる)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                // Lock準備
                if(!okuyamaClient.startTransaction()) {
                    throw new Exception("Transactionの開始に失敗");
                }

                long start = new Date().getTime();

                // Lock実行
                ret = okuyamaClient.lockData(args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
                if (ret[0].equals("true")) {
                    System.out.println("Lock成功");
                } else if (ret[0].equals("false")) {
                    System.out.println("Lock失敗");
                } 


                // 以下のコメントアウトをはずして、コンパイルし、
                // 別のクライアントから更新を実行すると、更新できないのが、わかる
                Thread.sleep(5000);

                // 自身でロックしているので更新可能
                if (!okuyamaClient.setValue(args[3], "LockDataValue")) {
                    System.out.println("登録失敗");
                }

                ret = okuyamaClient.getValue(args[3]);
                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println("Lock中に登録したデータ[" + ret[1] + "]");
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }

                // 自身でロックしているので削除可能
                ret = okuyamaClient.removeValue(args[3]);

                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println("Lock中に削除したデータ[" + ret[1] + "]");
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }

                // Lock開放
                ret = okuyamaClient.releaseLockData(args[3]);
                if (ret[0].equals("true")) {
                    System.out.println("Lock開放成功");
                } else if (ret[0].equals("false")) {
                    System.out.println("Lock開放失敗");
                } 

                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                // トランザクション開放
                okuyamaClient.endTransaction();
                okuyamaClient.close();
            } else if (args[0].equals("11")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存一度登録した値はエラー

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                String[] retParam = okuyamaClient.setNewValue(args[3], args[4]);
                if(retParam[0].equals("false")) {
                
                    System.out.println(retParam[1]);
                } else {

                    System.out.println("処理成功");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("12")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存一度登録した値はエラー
                // Tag有り

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                String[] tags = args[4].split(",");
                String[] retParam = okuyamaClient.setNewValue(args[3], tags, args[5]);
                if(retParam[0].equals("false")) {
                
                    System.out.println(retParam[1]);
                } else {

                    System.out.println("処理成功");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("13")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(getValueVersionCheck)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                if (args.length > 4) {
                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        ret = okuyamaClient.getValueVersionCheck("datasavekey_" + args[4] + "_" + new Integer(i).toString());
                        if (ret[0].equals("true")) {
                            // データ有り
                            System.out.println("Value=[" + ret[1] + "]");
                            System.out.println("VersionNo=[" + ret[2] + "]");
                        } else if (ret[0].equals("false")) {
                            System.out.println("データなし");
                        } else if (ret[0].equals("error")) {
                            System.out.println(ret[1]);
                            System.out.println(ret[2]);
                        }
                    }
                } else {
                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        ret = okuyamaClient.getValueVersionCheck("datasavekey_" + new Integer(i).toString());
                        if (ret[0].equals("true")) {
                            // データ有り
                            System.out.println(ret[1]);
                            System.out.println("VersionNo=[" + ret[2] + "]");
                        } else if (ret[0].equals("false")) {
                            System.out.println("データなし");
                        } else if (ret[0].equals("error")) {
                            System.out.println(ret[1]);
                            System.out.println(ret[2]);
                        }
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("13.1")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;

                long start = new Date().getTime();
                ret = okuyamaClient.getValueVersionCheck(args[3]);
                if (ret[0].equals("true")) {

                    // データ有り
                    System.out.println("Value=[" + ret[1] + "]");
                    System.out.println("VersionNo=[" + ret[2] + "]");
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                    System.out.println(ret[2]);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("14")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存
                // バージョンチェック有り

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                String[] retParam = okuyamaClient.setValueVersionCheck(args[3], args[4], args[5]);
                if(retParam[0].equals("false")) {
                
                    System.out.println(retParam[1]);
                } else {

                    System.out.println("処理成功");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("15")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存
                // バージョンチェック有り
                // Tag有り

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                String[] tags = args[4].split(",");
                String[] retParam = okuyamaClient.setValueVersionCheck(args[3], tags, args[5], args[6]);
                if(retParam[0].equals("false")) {
                
                    System.out.println(retParam[1]);
                } else {

                    System.out.println("処理成功");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("22")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;
                String[] multiKeys = null;
                
                if (args.length > 4) {
                    multiKeys = new String[Integer.parseInt(args[3])];
                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        multiKeys[i] = "datasavekey_" + args[4] + "_" + new Integer(i).toString();
                    }
                } else {
                    multiKeys = new String[Integer.parseInt(args[3])];
                    for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                        multiKeys[i] = "datasavekey_" + new Integer(i).toString();
                    }
                }
                
                long start = new Date().getTime();
                Map retMap = okuyamaClient.getMultiValue(multiKeys);
                long end = new Date().getTime();
                if (retMap == null) {
                    System.out.println(retMap);
                } else {

                    if (args.length > 4) {
                        for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                            String val = (String)retMap.get("datasavekey_" + args[4] + "_" + new Integer(i).toString());
                            if(val == null || !val.equals("savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol<MKIUJNBGTRFBVFREDCXSWQAZXSWEDCVFRTGBNHY678745_savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol<MKIUJNBGTRFBVFREDCXSWQAZXSWEDCVFRTGBNHY678745savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol" + args[4] + "_" + new Integer(i).toString())) {
                                System.out.println("Error - Key=[" + "datasavekey_" + args[4] + "_" + new Integer(i).toString() + " Value=[" + val + "]");
                            }
                        }
                    } else {
                        for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                           String val = (String)retMap.get("datasavekey_" + new Integer(i).toString());
                           if(val == null || !val.equals("savedatavaluestr_" + new Integer(i).toString())) {
                               System.out.println("Error - Key=[" + "datasavekey_" + new Integer(i).toString() + " Value=[" + val + "]");
                           }
                        }
                    }
                    System.out.println(retMap);
                    System.out.println("ResultSize = [" + retMap.size() + "]");
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();                
            } else if (args[0].equals("22.1")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] ret = null;
                String[] multiKeys = null;
                
                String targetKeysStr = args[3];
                // Key配列作成
                multiKeys = targetKeysStr.split(" ");
                
                long start = new Date().getTime();
                Map retMap = okuyamaClient.getMultiValue(multiKeys);
                long end = new Date().getTime();
                if (retMap == null) {
                    System.out.println(retMap);
                } else {
                    System.out.println(retMap);
                    System.out.println("ResultSize = [" + retMap.size() + "]");
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close(); 
            } else if (args[0].equals("23")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Keyのみ)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                
                long start = new Date().getTime();
                Map retMap = okuyamaClient.getTagValues(args[3]);
                long end = new Date().getTime();
                if (retMap == null) {
                    System.out.println(retMap);
                } else {
                    System.out.println(retMap);
                    System.out.println("ResultSize = [" + retMap.size() + "]");
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();  
            } else if (args[0].equals("24")) {

                // OkuyamaClientを使用してデータの加算を行う
                int port = Integer.parseInt(args[2]);

                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                
                long start = new Date().getTime();
                Object[] ret = okuyamaClient.incrValue(args[3], 1);
                long end = new Date().getTime();
                if (ret[0].equals("true")) {
                    System.out.println(ret[1]);
                } else {
                    System.out.println(ret[0]);
                    System.out.println(ret[1]);
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();  
            } else if (args[0].equals("24.1")) {

                // OkuyamaClientを使用してデータの加算を行う
                int port = Integer.parseInt(args[2]);

                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                
                long start = new Date().getTime();
                Object[] ret = null;
                for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                    ret = okuyamaClient.incrValue(args[4], 1);
                }
                long end = new Date().getTime();
                if (ret[0].equals("true")) {
                    System.out.println(ret[1]);
                } else {
                    System.out.println(ret[0]);
                    System.out.println(ret[1]);
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();  
            } else if (args[0].equals("25")) {

                // OkuyamaClientを使用してデータの減算を行う
                int port = Integer.parseInt(args[2]);

                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                
                long start = new Date().getTime();
                Object[] ret = okuyamaClient.decrValue(args[3], 1);
                long end = new Date().getTime();
                if (ret[0].equals("true")) {
                    System.out.println(ret[1]);
                } else {
                    System.out.println(ret[0]);
                    System.out.println(ret[1]);
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();  
            } else if (args[0].equals("25.1")) {

                // OkuyamaClientを使用してデータの加算を行う
                int port = Integer.parseInt(args[2]);

                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                
                long start = new Date().getTime();
                Object[] ret = null;
                for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                    ret = okuyamaClient.decrValue(args[4], 1);
                }
                long end = new Date().getTime();
                if (ret[0].equals("true")) {
                    System.out.println(ret[1]);
                } else {
                    System.out.println(ret[0]);
                    System.out.println(ret[1]);
                }
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();  
            } else if (args[0].equals("26")) {

                // OkuyamaClientを使用してKeyとTagを指定してKeyからTagを外す
                int port = Integer.parseInt(args[2]);

                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] tag1 = {"tag1"};
                String[] tag2 = {"tag1","tag2"};
                String[] tag3 = {"tag1","tag2","tag3"};
                String[] tag4 = {"tag4"};
                String[] setTag = null;
                int counter = 0;

                long start = new Date().getTime();

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

                    for (int ii = 0; ii < setTag.length; ii++) {
                        if (!okuyamaClient.removeTagFromKey("tagsampledatakey_" + new Integer(i).toString(), setTag[ii])) {
                            System.out.println("OkuyamaClient - error Key=[" + "tagsampledatakey_" + new Integer(i) + "] Tag[" + setTag[ii] +"]");
                        }
                    }
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("27")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);

                Random rnd = new Random();
                StringBuilder strBuf =null; 
                if (args.length > 4) {
                    strBuf = new StringBuilder(6000*10);
                    for (int i = 0; i < 3000; i++) {
                        strBuf.append(rnd.nextInt(1999999999));
                    }
                }

                long start = new Date().getTime();
                for (int i = 0; i < Integer.parseInt(args[3]);i++) {
                    // データ登録
                    
                    //if (!okuyamaClient.setValue("datasavekey_" + args[4] + "_" + new Integer(i).toString(), "savedatavaluestr_" + args[4] + "_" + new Integer(i).toString())) {
                    if (!okuyamaClient.setValueAndCreateIndex("datasavekey_" + args[4] + "_" + new Integer(i).toString(), "savedatavaluestr0987654321qazxswedcvfrtgbnhyujm,kiol<MKIUJNBGTRFBVFREDCXSWQAZXSWEDCVFRTGBNHY678745_savedatavaluestr09876543" + args[4] + "_" + new Integer(i).toString())) {
                    //if (!okuyamaClient.setValue("datasavekey_" + args[4] + "_" + new Integer(i).toString(), "savedatavaluestr0987654321" + strBuf.toString() + "_" + args[4] + "_" + new Integer(i).toString())) {
                        System.out.println("OkuyamaClient - error");
                    } else {
                        //System.out.println("Store[" + "datasavekey_" + args[4] + "_" + new Integer(i).toString() + "]");
                    }
                    //if ((i % 1000) == 0) System.out.println(i);
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } if (args[0].equals("27.1")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータをIndexを作りながら保存

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                // Key, Value, Prefix
                if (!okuyamaClient.setValueAndCreateIndex(args[3], args[4])) {
                //if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("OkuyamaClient - error");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();

            } if (args[0].equals("27.2")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータをIndexを作りながら保存

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                // Key, Value, Prefix
                if (!okuyamaClient.setValueAndCreateIndex(args[3], args[4], args[5])) {
                //if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("OkuyamaClient - error");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } if (args[0].equals("27.3")) {
                
                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータをIndexを作りながら保存(Prefixあり)

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                
                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);


                long start = new Date().getTime();
                // Key, Tag, Value, Prefix
                String[] tags = {args[4]};
                if (!okuyamaClient.setValueAndCreateIndex(args[3], tags, args[5], args[6])) {
                //if (!okuyamaClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("OkuyamaClient - error");
                }
                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();
            } else if (args[0].equals("27.4")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを保存(Tagなし)

                // クライアントインスタンスを作成
                OkuyamaClient okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(args[1], port);

                Random rnd = new Random();


                long start = new Date().getTime();

                int idx = 0;

                FileInputStream workKeyFilefis = null;
                InputStreamReader isr = null;

                FileReader fr = null;
                BufferedReader br = null;

                File dataFile = new File(args[3]);
                workKeyFilefis = new FileInputStream(dataFile);
                isr = new InputStreamReader(workKeyFilefis , "UTF-8");
                br = new BufferedReader(isr);

                String line = null;


                while ((line = br.readLine()) != null) {
                    idx++;
                    // データ登録
                    if (!okuyamaClient.setValueAndCreateIndex("key_" + idx, line)) {
                        System.out.println("OkuyamaClient - error");
                    } else {
                        //System.out.println("Store[key_" + idx + "]");
                    }
                    if ((idx % 1000) == 0) System.out.println(idx);
                }

                long end = new Date().getTime();
                System.out.println((end - start) + "milli second");

                okuyamaClient.close();

            } else if (args[0].equals("28")) {

                int port = Integer.parseInt(args[2]);
                // OkuyamaClientを使用してデータを取得(Tagでの取得)
                OkuyamaClient okuyamaClient = new OkuyamaClient();
                okuyamaClient.connect(args[1], port);
                String[] keys = null;
                String[] searchCharList = args[3].split(":");
                long start = new Date().getTime();

                Object[] ret = okuyamaClient.searchValue(searchCharList, args[4]);
                long end = new Date().getTime();

                if (ret[0].equals("true")) {
                    // データ有り
                    System.out.println((end - start) + " mille");
                    keys = (String[])ret[1];
                    System.out.println("Result Count[" + keys.length + "]");
                    /*for (int idx = 0; idx < keys.length; idx++) {
                        System.out.println(keys[idx]);
                    }*/
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし");
                } else if (ret[0].equals("error")) {
                    System.out.println(ret[1]);
                }

/*
                if (keys != null) {
                    for (int ii = 0; ii < keys.length; ii++) {
                        System.out.println("Key=[" + keys[ii] + "]");
                        ret = okuyamaClient.getValue(keys[ii]);
                        System.out.println("Value=[" + ret[1] + "]");
                    }
                }
                */
                end = new Date().getTime();
                System.out.println((end - start));
                okuyamaClient.close();

            } 
            




        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}