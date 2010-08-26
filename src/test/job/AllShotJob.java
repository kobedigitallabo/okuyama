package test.job;

import java.io.*;
import java.net.*;
import java.util.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractJob;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.StatusUtil;
import org.imdst.util.JavaSystemApi;
import org.imdst.client.*;

/**
 * 登録、取得、Tag登録、Tag取得、削除、Script実行、一意登録、のテストを実行
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class AllShotJob extends AbstractJob implements IJob {

    private String masterNodeName = "127.0.0.1";
    private int masterNodePort = 8888;

    // 初期化メソッド定義
    public void initJob(String initValue) {
        if (initValue != null && !initValue.equals("")) {
            String[] master = initValue.split(":");
            masterNodeName = master[0];
            masterNodePort = Integer.parseInt(master[1]);
        }
    }


    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {

        String ret = SUCCESS;

        String[] execMethods = null;
        int count = 1000;
        HashMap retMap = new HashMap();

        try{
            execMethods = optionParam.split(",");

            for (int i = 0; i < execMethods.length; i++) {

                if (execMethods[i].equals("set")) 
                    retMap.put("set", execSet(null, count));

                if (execMethods[i].equals("get")) 
                    retMap.put("get", execGet(null, count));

                if (execMethods[i].equals("settag")) 
                    retMap.put("settag", execTagSet(null, count));

                if (execMethods[i].equals("gettag")) 
                    retMap.put("gettag", execTagGet(null, count));

                if (execMethods[i].equals("remove")) 
                    retMap.put("remove", execRemove(null, 500));

                if (execMethods[i].equals("script")) 
                    retMap.put("script", execScript(null, count));

                if (execMethods[i].equals("add")) 
                    retMap.put("add", execAdd(null, count));
            }

            System.out.println(retMap);
        } catch(Exception e) {
            System.out.println(retMap);
            throw new BatchException(e);
        }

        return ret;
    }


    private boolean execSet(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execSet - Start");
            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            long start = new Date().getTime();
            for (int i = 0; i < count; i++) {
                // データ登録

                if (!imdstKeyValueClient.setValue("datasavekey_" + new Integer(i).toString(), "savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("Set - Error=[" + "datasavekey_" + new Integer(i).toString() + ",  savedatavaluestr_" + new Integer(i).toString());
                    errorFlg = true;
                }
                if ((i % 10000) == 0) System.out.println(i);
            }
            long end = new Date().getTime();
            System.out.println("Set Method= " + (end - start) + " milli second");

            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execSet - End");

        return errorFlg;
    }


    private boolean execGet(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execGet - Start");

            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            String[] ret = null;

            long start = new Date().getTime();
            for (int i = 0; i < count; i++) {
                ret = imdstKeyValueClient.getValue("datasavekey_" + new Integer(i).toString());

                if (ret[0].equals("true")) {
                    // データ有り
                    //System.out.println(ret[1]);
                    if (!ret[1].equals("savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("データが合っていない key=[" + "datasavekey_" + new Integer(i).toString() + "]  value=[" + ret[1] + "]");
                        errorFlg = true;
                    }
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし key=[" + "datasavekey_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } else if (ret[0].equals("error")) {
                    System.out.println("Error key=[" + "datasavekey_" + new Integer(i).toString() + "]" + ret[1]);
                    errorFlg = true;
                }
            }
            long end = new Date().getTime();
            System.out.println("Get Method= " + (end - start) + " milli second");

            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execGet - End");
        return errorFlg;
    }



    private boolean execTagSet(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execTagSet - Start");
            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            String[] tag1 = {"tag1"};
            String[] tag2 = {"tag1","tag2"};
            String[] tag3 = {"tag1","tag2","tag3"};
            String[] tag4 = {"tag4"};
            String[] setTag = null;
            int counter = 0;

            long start = new Date().getTime();

            for (int i = 0; i < count; i++) {
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
                    System.out.println("Tag Set - Error=[tagsampledatakey_" + new Integer(i).toString() + ",  tagsamplesavedata_" + new Integer(i).toString());
                    errorFlg = true;
                }
            }
            long end = new Date().getTime();
            System.out.println("Tag Set Method= " + (end - start) + " milli second");

            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execTagSet - End");
        return errorFlg;
    }


    private boolean execTagGet(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execTagGet - Start");

            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            String[] tag1 = {"tag1"};
            String[] tag2 = {"tag1","tag2"};
            String[] tag3 = {"tag1","tag2","tag3"};
            String[] tag4 = {"tag4"};
            String[] setTag = null;

            String[] keys = null;
            long start = new Date().getTime();
            Object[] ret = imdstKeyValueClient.getTagKeys("tag1");

            if (ret[0].equals("true")) {
                // データ有り
                keys = (String[])ret[1];

                for (int ii = 0; ii < keys.length; ii++) {
                    String[] getRet = imdstKeyValueClient.getValue(keys[ii]);

                    if (getRet[0].equals("true")) {
                        // データ有り
                        //System.out.println(getRet[1]);
                    } else if (getRet[0].equals("false")) {
                        System.out.println("データなし key=[" + keys[ii] + "]");
                        errorFlg = true;
                    } else if (getRet[0].equals("error")) {
                        System.out.println("Error key=[" + keys[ii] + "]");
                        errorFlg = true;
                    }
                }

            } else if (ret[0].equals("false")) {
                System.out.println("tag1=データなし");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("tag1=Error[" + ret[1] + "]");
                errorFlg = true;
            }

            long end = new Date().getTime();
            System.out.println("Tag Get Method= " + (end - start) + " milli second");

            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execTagGet - End");
        return errorFlg;
    }



    private boolean execRemove(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execRemove - Start");

            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            String[] ret = null;

            long start = new Date().getTime();
            for (int i = 0; i < count;i++) {
                ret = imdstKeyValueClient.removeValue("datasavekey_" + new Integer(i).toString());
                if (ret[0].equals("true")) {
                    // データ有り
                    //System.out.println(ret[1]);
                    if (!ret[1].equals("savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("データが合っていない key=[" + "datasavekey_" + new Integer(i).toString() + "]  value=[" + ret[1] + "]");
                        errorFlg = true;
                    }
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし key=[" + "datasavekey_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } else if (ret[0].equals("error")) {
                    System.out.println("Error key=[" + "datasavekey_" + new Integer(i).toString() + "]" + ret[1]);
                    errorFlg = true;
                }

            }
            long end = new Date().getTime();
            System.out.println("Remove Method= " + (end - start) + " milli second");


            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execRemove - End");
        return errorFlg;
    }



    private boolean execScript(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execScript - Start");

            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            long start = new Date().getTime();
            String[] ret = imdstKeyValueClient.getValueScript("datasavekey_600", "var dataValue; var retValue = dataValue.replace('data', 'dummy'); var execRet = '1';");
            if (ret[0].equals("true")) {
                // データ有り
                //System.out.println(ret[1]);
                if (!ret[1].equals("savedummyvaluestr_600")) {
                    System.out.println("データが合っていない" + ret[1]);
                    errorFlg = true;
                }
            } else if (ret[0].equals("false")) {
                System.out.println("データなし key=[" + "datasavekey_600]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error key=[" + "datasavekey_600]");
                errorFlg = true;
            }

            long end = new Date().getTime();
            System.out.println("GetScript Method= " + (end - start) + " milli second");

            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execScript - End");
        return errorFlg;
    }


    private boolean execAdd(ImdstKeyValueClient client, int count) throws Exception {
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execAdd - Start");

            if (client != null) {
                imdstKeyValueClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                imdstKeyValueClient = new ImdstKeyValueClient();

                // マスタサーバに接続
                imdstKeyValueClient.connect(masterNodeName, port);
            }

            long start = new Date().getTime();
            String[] retParam = imdstKeyValueClient.setNewValue("Key_ABCDE", "AAAAAAAAABBBBBBBBBBBBCCCCCCCCCC");

            if(retParam[0].equals("false")) {

                System.out.println("Key=[Key_ABCDE] Error=[" + retParam[1] + "]");
                errorFlg = true;
            } else {
                //System.out.println("処理成功");
            }
            long end = new Date().getTime();

            System.out.println("New Value Method= " + (end - start) + " milli second");

            if (client == null) {
                imdstKeyValueClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execAdd - End");
        return errorFlg;
    }
}