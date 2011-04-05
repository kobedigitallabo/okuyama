package test.job;

import java.io.*;
import java.net.*;
import java.util.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.client.*;

/**
 * 登録、取得、Tag登録、Tag取得、削除、Script実行、一意登録、のテストを実行
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MethodPatterTestJob extends AbstractJob implements IJob {

    private String masterNodeName = "127.0.0.1";
    private int masterNodePort = 8888;

    private int nowCount = 0;

    private ILogger logger = LoggerFactory.createLogger(MethodPatterTestJob.class);

    private String bigCharacter = "";



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
        int count = 5000;
        HashMap retMap = new HashMap();

        try{
            execMethods = optionParam.split(",");

            int port = masterNodePort;

            // クライアントインスタンスを作成
            OkuyamaClient okuyamaClient = null;

            String startStr = super.getPropertiesValue(super.getJobName() + "start");
            int start = Integer.parseInt(startStr);
            count = count + start;
            for (int cy = 0; cy < 1; cy++) {
                for (int t = 0; t < Integer.parseInt(execMethods[0]); t++) {
                    StringBuilder strBuf = new StringBuilder(6000*10);
                    Random rnd = new Random();

                    for (int i = 0; i < 3000; i++) {
                        strBuf.append(rnd.nextInt(1999999999));
                    }
                    bigCharacter = strBuf.toString();
                    this.nowCount = t;
                    System.out.println("Test Count =[" + t + "]");
                    for (int i = 1; i < execMethods.length; i++) {

                        if (execMethods[i].equals("set")) 
                            retMap.put("set", execSet(okuyamaClient, start, count));

                        if (execMethods[i].equals("get")) 
                            retMap.put("get", execGet(okuyamaClient, start, count));

                        if (execMethods[i].equals("settag")) 
                            retMap.put("settag", execTagSet(okuyamaClient, start, count));

                        if (execMethods[i].equals("gettag")) 
                            retMap.put("gettag", execTagGet(okuyamaClient, start, count));

                        if (execMethods[i].equals("remove")) 
                            retMap.put("remove", execRemove(okuyamaClient, start, 500));

                        if (execMethods[i].equals("script")) 
                            retMap.put("script", execScript(okuyamaClient, start, count));

                        if (execMethods[i].equals("add")) 
                            retMap.put("add", execAdd(okuyamaClient, start, count));

                        if (execMethods[i].equals("gets-cas")) 
                            retMap.put("cas", execGetsCas(okuyamaClient, start, count));

                        if (execMethods[i].equals("incr")) 
                            retMap.put("incr", execIncr(okuyamaClient, start, count));

                        if (execMethods[i].equals("decr")) 
                            retMap.put("decr", execDecr(okuyamaClient, start, count));

                        if (execMethods[i].equals("tagremove")) 
                            retMap.put("tagremove", execTagRemove(okuyamaClient, start, count));

                    }

                    System.out.println("ErrorMap=" + retMap.toString());
                    System.out.println("---------------------------------------------");
                    // クライアントインスタンスを作成
                    okuyamaClient = new OkuyamaClient();
                    // マスタサーバに接続
                    okuyamaClient.connect(masterNodeName, port);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println(new Date());
            System.out.println(retMap);
            throw new BatchException(e);
        }

        return ret;
    }


    private boolean execSet(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execSet - Start");
            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            long startTime = new Date().getTime();
            for (int i = start; i < count; i++) {
                // データ登録

                if (!okuyamaClient.setValue(this.nowCount + "datasavekey_" + new Integer(i).toString(), this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString())) {
                    System.out.println("Set - Error=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + ", " + this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString());
                    errorFlg = true;
                }
                if ((i % 10000) == 0) System.out.println(i);
            }

            for (int i = start; i < start+500; i++) {
                // データ登録

                if (!okuyamaClient.setValue(this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString(), this.nowCount + "savetestbigdata_" + new Integer(i).toString() + "_" + bigCharacter + "_" + new Integer(i).toString())) {
                    System.out.println("Set - Error=[" + this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString());
                    errorFlg = true;
                }
                if ((i % 10000) == 0) System.out.println(i);
            }
            long endTime = new Date().getTime();
            System.out.println("Set Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execSet - End");

        return errorFlg;
    }


    private boolean execGet(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execGet - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] ret = null;

            long startTime = new Date().getTime();
            for (int i = start; i < count; i++) {
                ret = okuyamaClient.getValue(this.nowCount + "datasavekey_" + new Integer(i).toString());

                if (ret[0].equals("true")) {
                    // データ有り
                    //System.out.println(ret[1]);
                    if (!ret[1].equals(this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("データが合っていない key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]  value=[" + ret[1] + "]");
                        errorFlg = true;
                    }
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]");
                    logger.error("データなし key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } else if (ret[0].equals("error")) {
                    System.out.println("Error key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]" + ret[1]);
                    errorFlg = true;
                }
            }


            for (int i = start; i < start+500; i++) {
                ret = okuyamaClient.getValue(this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString());

                if (ret[0].equals("true")) {
                    // データ有り
                    //System.out.println(ret[1]);
                    if (!ret[1].equals(this.nowCount + "savetestbigdata_" + new Integer(i).toString() + "_" + bigCharacter + "_" + new Integer(i).toString())) {
                        System.out.println("データが合っていない key=[" + this.nowCount + "savetestbigdata_" + new Integer(i).toString() + "]  value=[" + ret[1] + "]");
                        errorFlg = true;
                    }
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし key=[" + this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString() + "]");
                    logger.error("データなし key=[" + this.nowCount + "datasavekey_bigdata" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } else if (ret[0].equals("error")) {
                    System.out.println("Error key=[" + this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString() + "]" + ret[1]);
                    errorFlg = true;
                }
            }


            String[] keys = new String[1000];
            int idx = 0;
            Map checkResultMap = new HashMap(1000);
            for (int i = start; i < start+500; i++) {
                keys[idx] = this.nowCount + "datasavekey_" + new Integer(i).toString();
                checkResultMap.put(keys[idx], this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString());
                keys[idx+1] = this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString();
                checkResultMap.put(keys[idx+1], this.nowCount + "savetestbigdata_" + new Integer(i).toString() + "_" + bigCharacter + "_" + new Integer(i).toString());
                idx = idx+2;
            }

            Map multiGetRet = okuyamaClient.getMultiValue(keys);

            if (!checkResultMap.equals(multiGetRet)) {
                System.out.println("データなし MultiGet=[" + multiGetRet + "]");
                logger.error("データなし MultiGet=[" + multiGetRet + "]");
                errorFlg = true;
            }
            long endTime = new Date().getTime();
            System.out.println("Get Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execGet - End");
        return errorFlg;
    }



    private boolean execTagSet(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execTagSet - Start");
            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] tag1 = {start+"_" + this.nowCount + "_tag1"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1",start+"_" + this.nowCount + "_tag2"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1",start+"_" + this.nowCount + "_tag2",start+"_" + this.nowCount + "_tag3"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4"};
            String[] setTag = null;
            int counter = 0;

            long startTime = new Date().getTime();

            for (int i = start; i < count; i++) {
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

                if (!okuyamaClient.setValue(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag, this.nowCount + "tagsamplesavedata_" + new Integer(i).toString())) {
                    System.out.println("Tag Set - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", " + this.nowCount + "tagsamplesavedata_" + new Integer(i).toString());
                    errorFlg = true;
                }
            }
            long endTime = new Date().getTime();
            System.out.println("Tag Set Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execTagSet - End");
        return errorFlg;
    }


    private boolean execTagGet(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execTagGet - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] tag1 = {start+"_" + this.nowCount + "_tag1"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1",start+"_" + this.nowCount + "_tag2"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1",start+"_" + this.nowCount + "_tag2",start+"_" + this.nowCount + "_tag3"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4"};
            String[] setTag = null;

            ArrayList tag1RetList = new ArrayList();
            ArrayList tag2RetList = new ArrayList();
            ArrayList tag3RetList = new ArrayList();
            ArrayList tag4RetList = new ArrayList();
            int counter = 0;
            for (int i = start; i < count; i++) {
                if (counter == 0) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 1) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    tag2RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 2) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    tag2RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    tag3RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 3) {

                    tag4RetList.add(this.nowCount + "tagsampledatakey_" + new Integer(i).toString());
                    counter = 0;
                }
            }

            HashMap getResult1 = new HashMap();
            HashMap getResult2 = new HashMap();
            HashMap getResult3 = new HashMap();
            HashMap getResult4 = new HashMap();
            String[] keys = null;
            long startTime = new Date().getTime();
            Object[] ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag1");

            if (ret[0].equals("true")) {
                // データ有り
                keys = (String[])ret[1];

                for (int ii = 0; ii < keys.length; ii++) {
                    getResult1.put(keys[ii], "*");

                }

            } else if (ret[0].equals("false")) {
                System.out.println(start+"tag1=データなし");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println(start+"tag1=Error[" + ret[1] + "]");
                errorFlg = true;
            }

            ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag2");

            if (ret[0].equals("true")) {
                // データ有り
                keys = (String[])ret[1];


                for (int ii = 0; ii < keys.length; ii++) {

                    getResult2.put(keys[ii], "*");
                }

            } else if (ret[0].equals("false")) {
                System.out.println(start+"_tag2=データなし");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println(start+"tag2=Error[" + ret[1] + "]");
                errorFlg = true;
            }


            ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag3");

            if (ret[0].equals("true")) {
                // データ有り
                keys = (String[])ret[1];

                for (int ii = 0; ii < keys.length; ii++) {
                    getResult3.put(keys[ii], "*");
                }

            } else if (ret[0].equals("false")) {
                System.out.println(start+"_tag3=データなし");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println(start+"tag3=Error[" + ret[1] + "]");
                errorFlg = true;
            }


            ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag4");

            if (ret[0].equals("true")) {
                // データ有り
                keys = (String[])ret[1];

                for (int ii = 0; ii < keys.length; ii++) {
                    getResult4.put(keys[ii], "*");
                }

            } else if (ret[0].equals("false")) {
                System.out.println(start+"_tag4=データなし");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println(start+"tag4=Error[" + ret[1] + "]");
                errorFlg = true;
            }


            // 検証
            // Tag1
            for (int idx = 0; idx < tag1RetList.size(); idx++) {
                if (!getResult1.containsKey((String)tag1RetList.get(idx))) {
                    System.out.println(start+"_tag1=該当データなし Key=[" + (String)tag1RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag2
            for (int idx = 0; idx < tag2RetList.size(); idx++) {
                if (!getResult2.containsKey((String)tag2RetList.get(idx))) {
                    System.out.println(start+"_tag2=該当データなし Key=[" + (String)tag2RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag3
            for (int idx = 0; idx < tag3RetList.size(); idx++) {
                if (!getResult3.containsKey((String)tag3RetList.get(idx))) {
                    System.out.println(start+"_tag3=該当データなし Key=[" + (String)tag3RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag4
            for (int idx = 0; idx < tag4RetList.size(); idx++) {
                if (!getResult4.containsKey((String)tag4RetList.get(idx))) {
                    System.out.println(start+"_tag4=該当データなし Key=[" + (String)tag4RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            long endTime = new Date().getTime();
            System.out.println("Tag Get Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execTagGet - End");
        return errorFlg;
    }



    private boolean execRemove(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execRemove - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] ret = null;

            long startTime = new Date().getTime();
            for (int i = start; i < count;i++) {
                ret = okuyamaClient.removeValue(this.nowCount + "datasavekey_" + new Integer(i).toString());
                if (ret[0].equals("true")) {
                    // データ有り
                    //System.out.println(ret[1]);
                    if (!ret[1].equals(this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString())) {
                        System.out.println("データが合っていない key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]  value=[" + ret[1] + "]");
                        errorFlg = true;
                    }
                } else if (ret[0].equals("false")) {
                    System.out.println("データなし key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } else if (ret[0].equals("error")) {
                    System.out.println("Error key=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + "]" + ret[1]);
                    errorFlg = true;
                }

            }
            long endTime = new Date().getTime();
            System.out.println("Remove Method= " + (endTime - startTime) + " milli second");


            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execRemove - End");
        return errorFlg;
    }



    private boolean execScript(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execScript - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            long startTime = new Date().getTime();
            String[] ret = okuyamaClient.getValueScript(this.nowCount + "datasavekey_" + (start + 600), "var dataValue; var retValue = dataValue.replace('data', 'dummy'); var execRet = '1';");
            if (ret[0].equals("true")) {
                // データ有り
                //System.out.println(ret[1]);
                if (!ret[1].equals(this.nowCount + "savedummyvaluestr_" +  (start + 600))) {
                    System.out.println("データが合っていない" + ret[1]);
                    errorFlg = true;
                }
            } else if (ret[0].equals("false")) {
                System.out.println("データなし key=[" + this.nowCount + "datasavekey_" + (start + 600));
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error key=[" + this.nowCount + "datasavekey_" +  (start + 600));
                errorFlg = true;
            }

            long endTime = new Date().getTime();
            System.out.println("GetScript Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execScript - End");
        return errorFlg;
    }


    private boolean execAdd(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execAdd - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            long startTime = new Date().getTime();
            String[] retParam = okuyamaClient.setNewValue(this.nowCount + "Key_ABCDE" + start, this.nowCount + "AAAAAAAAABBBBBBBBBBBBCCCCCCCCCC" + start);

            if(retParam[0].equals("false")) {

                System.out.println("Key=[" + this.nowCount + "Key_ABCDE] Error=[" + retParam[1] + "]");
                errorFlg = true;
            } else {
                //System.out.println("処理成功");
            }
            long endTime = new Date().getTime();

            System.out.println("New Value Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execAdd - End");
        return errorFlg;
    }


    private boolean execGetsCas(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execCas - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            long startTime = new Date().getTime();
            int casCount = 0;
            for (int i = start; i < count; i++) {
                // データ登録

                if (!okuyamaClient.setValue(casCount + "_castest_datasavekey", "castest_testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + casCount + "_" + new Integer(i).toString())) {
                    System.out.println("Set - Error=[" + casCount + "_castest_datasavekey] Value[" + this.nowCount + "castest_testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + casCount + "_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                }
                casCount++;
            }

            Random rndIdx = new Random();
            int casSuccessCount = 0;
            int casErrorCount = 0;
            for (int casIdx = 0; casIdx < 6000; casIdx++) {
                int rndSet = rndIdx.nextInt(casCount);
                Object[] getsRet = okuyamaClient.getValueVersionCheck(rndSet + "_castest_datasavekey");


                String[] retParam = okuyamaClient.setValueVersionCheck(rndSet + "_castest_datasavekey", "updated-" + rndSet, (String)getsRet[2]);
                if(retParam[0].equals("true")) {
                    casSuccessCount++;
                } else {
                    casErrorCount++;
                }
            }
            long endTime = new Date().getTime();

            System.out.println("Cas Method= " + (endTime - startTime) + " milli second Suucess=" + casSuccessCount + "  Error=" + casErrorCount);

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execCas - End");
        return errorFlg;
    }


    private boolean execIncr(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execIncr - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            long startTime = new Date().getTime();
            String[] work = okuyamaClient.setNewValue("calcKeyIncr", "0");


            for (int i = start; i < count; i++) {
                Object[] ret = okuyamaClient.incrValue("calcKeyIncr", 1);
                if (ret[0].equals("false")) {
                    errorFlg = true;
                    System.out.println(ret[0]);
                    System.out.println(ret[1]);
                }
            }

            long endTime = new Date().getTime();
            String[] nowVal = okuyamaClient.getValue("calcKeyIncr");
            System.out.println("IncrValue Method= " + (endTime - startTime) + " milli second Value=[" + nowVal[1] + "]");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execAdd - End");
        return errorFlg;
    }

    private boolean execDecr(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execDecr - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            long startTime = new Date().getTime();
            String[] work = okuyamaClient.setNewValue("calcKeyDecr", "1000000");


            for (int i = start; i < count; i++) {
                Object[] ret = okuyamaClient.decrValue("calcKeyDecr", 1);
                if (ret[0].equals("false")) {
                    errorFlg = true;
                    System.out.println(ret[0]);
                    System.out.println(ret[1]);
                }
            }

            long endTime = new Date().getTime();
            String[] nowVal = okuyamaClient.getValue("calcKeyDecr");
            System.out.println("DecrValue Method= " + (endTime - startTime) + " milli second Value=[" + nowVal[1] + "]");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execDecr - End");
        return errorFlg;
    }

    private boolean execTagRemove(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execTagRemove - Start");
            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] tag1 = {start+"_" + this.nowCount + "_tag1"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1",start+"_" + this.nowCount + "_tag2"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1",start+"_" + this.nowCount + "_tag2",start+"_" + this.nowCount + "_tag3"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4"};
            String[] setTag = null;
            int counter = 0;

            long startTime = new Date().getTime();

            for (int i = start; i < count; i++) {
                if (counter == 0) {

                    setTag = tag1;
                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[0])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[0]);
                        errorFlg = true;
                    }

                    counter++;
                } else if (counter == 1) {
                    setTag = tag2;

                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[0])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[0]);
                        errorFlg = true;
                    }

                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[1])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[1]);
                        errorFlg = true;
                    }

                    counter++;
                } else if (counter == 2) {
                    setTag = tag3;

                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[0])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[0]);
                        errorFlg = true;
                    }

                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[1])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[1]);
                        errorFlg = true;
                    }

                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[2])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[2]);
                        errorFlg = true;
                    }

                    counter++;
                } else if (counter == 3) {
                    setTag = tag4;

                    if (!okuyamaClient.removeTagFromKey(this.nowCount + "tagsampledatakey_" + new Integer(i).toString(), setTag[0])) {
                        System.out.println("TagRemove - Error=[" + this.nowCount + "tagsampledatakey_" + new Integer(i).toString() + ", TagRemove[" + setTag[0]);
                        errorFlg = true;
                    }
                    counter = 0;
                }

            }
            long endTime = new Date().getTime();
            System.out.println("Tag Remove Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execTagRemove - End");
        return errorFlg;
    }

}