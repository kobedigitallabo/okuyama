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
 * テストを実行.<be>
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
            String listNamePrefix = super.getPropertiesValue(super.getJobName() + "list");
            int start = Integer.parseInt(startStr);
            count = count + start;
            for (int cy = 0; cy < 1; cy++) {
                for (int t = 0; t < Integer.parseInt(execMethods[0]); t++) {
                    StringBuilder strBuf = new StringBuilder(6000*10);
                    Random rnd = new Random();

                    for (int i = 0; i < 300; i++) {
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

                        if (execMethods[i].equals("index")) 
                            retMap.put("createindex", execIndex(okuyamaClient, start, count));

                        if (execMethods[i].equals("setexpireandget")) 
                            retMap.put("setexpireandget", execSetExpireAndGet(okuyamaClient, start, count));

                        if (execMethods[i].equals("getmultitagvalues")) 
                            retMap.put("getmultitagvalues", execGetMultiTagValues(okuyamaClient, start, count));

                        if (execMethods[i].equals("objectsetget")) 
                            retMap.put("objectsetget", execObjectSetGet(okuyamaClient, start, count));

                        if (execMethods[i].equals("getmultitagkeys")) 
                            retMap.put("getmultitagkeys", execGetMultiTagKeys(okuyamaClient, start, count));

                        if (execMethods[i].equals("gettagkeysresult")) 
                            retMap.put("gettagkeysresult", execGetTagKeysResult(okuyamaClient, start, count));

                        if (execMethods[i].equals("getmultitagkeysresult")) 
                            retMap.put("getmultitagkeysresult", execMultiGetTagKeysResult(okuyamaClient, start, count));
                            
                        if (execMethods[i].equals("list"))
                            retMap.put("list", execList(okuyamaClient, start, listNamePrefix));

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


            String[] keys = new String[102];
            int idx = 0;
            Map checkResultMap = new HashMap(50);
            for (int i = start; i < start+50; i++) {
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


            Map newObjMap = new HashMap();
            newObjMap.put(this.nowCount + "Key_ABCDE_obj_" + start, this.nowCount + "AAAAAAAAABBBBBBBBBBBBCCCCCCCCCC" + start);

            String[] setNewObjRet = okuyamaClient.setNewObjectValue(this.nowCount + "Key_ABCDE_obj_" + start, newObjMap);
            if(!setNewObjRet[0].equals("true")) {

                System.out.println("Key=[" + this.nowCount + "Key_ABCDE_obj_" + start + "] Error =[" + setNewObjRet[1] + "]");
                errorFlg = true;
            } else {
                Object[] getNewObjRet = okuyamaClient.getObjectValue(this.nowCount + "Key_ABCDE_obj_" + start);
                if (getNewObjRet[0].equals("true")) {
                    Map retMap = (Map)getNewObjRet[1];
                    if (!retMap.equals(newObjMap)) {
                        System.out.println("Key=[" + this.nowCount + "Key_ABCDE_obj_" + start + "] Get - Error");
                        errorFlg = true;
                    } else {
                        //System.out.println("処理成功");
                    }
                }
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

            Object[] ret = okuyamaClient.incrValue("calcKeyIncr_" + this.nowCount, 1);
            if (ret[0].equals("true")) {
                System.out.println("execIncr - error - 1");
                errorFlg = true;
            } 
            ret = okuyamaClient.incrValue("calcKeyIncr_" + this.nowCount, 20, true);
            if (ret[0].equals("false")) {
                System.out.println("execIncr - error - 2");
                errorFlg = true;
            }

            long startTime = new Date().getTime();
            String[] work = okuyamaClient.setNewValue("calcKeyIncr", "0");


            for (int i = start; i < count; i++) {
                Object[] calcRet = okuyamaClient.incrValue("calcKeyIncr", 1);
                if (calcRet[0].equals("false")) {
                    errorFlg = true;
                    System.out.println(calcRet[0]);
                    System.out.println(calcRet[1]);
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

            Object[] ret = okuyamaClient.decrValue("calcKeyDecr_" + this.nowCount, 1);
            if (ret[0].equals("true")) {
                System.out.println("execDecr - error - 1");
                errorFlg = true;
            } 
            ret = okuyamaClient.decrValue("calcKeyDecr_" + this.nowCount, 20, true);
            if (ret[0].equals("false")) {
                System.out.println("execDecr - error - 2");
                errorFlg = true;
            }

            long startTime = new Date().getTime();
            String[] work = okuyamaClient.setNewValue("calcKeyDecr", "1000000");


            for (int i = start; i < count; i++) {
                Object[] calcRet = okuyamaClient.decrValue("calcKeyDecr", 1);
                if (calcRet[0].equals("false")) {
                    errorFlg = true;
                    System.out.println(calcRet[0]);
                    System.out.println(calcRet[1]);
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

            // 削除を検証
            Object[] ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag1");

            if (ret[0].equals("true")) {
                // データ有り
                System.out.println("TagRemoveDataExsist[" + start+"_" + this.nowCount + "_tag1]" );
                errorFlg = true;
            }

            ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag2");

            if (ret[0].equals("true")) {
                // データ有り
                System.out.println("TagRemoveDataExsist[" + start+"_" + this.nowCount + "_tag2]" );
                errorFlg = true;
            }


            ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag3");

            if (ret[0].equals("true")) {
                // データ有り
                System.out.println("TagRemoveDataExsist[" + start+"_" + this.nowCount + "_tag3]" );
                errorFlg = true;
            }

            ret = okuyamaClient.getTagKeys(start+"_" + this.nowCount + "_tag4");

            if (ret[0].equals("true")) {
                // データ有り
                System.out.println("TagRemoveDataExsist[" + start+"_" + this.nowCount + "_tag4]" );
                errorFlg = true;
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


    private boolean execIndex(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            String[] sChars = null;
            Object[] searchRet = null;
            System.out.println("execIndex - Start");
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
            okuyamaClient.setValueAndCreateIndex(this.nowCount + "test123", "あいうえおかきくけこ");
            String[] dByteChars = {"うえ","かき"};
            Object[] dByteRet = okuyamaClient.searchValue(dByteChars, "1");
            if(((String)dByteRet[0]).equals("true"))  {
                String[] retkeys = (String[])dByteRet[1];
                boolean matchFlg = false;
                for (int i= 0; i < retkeys.length; i++) {
                    if (retkeys[i].equals(this.nowCount + "test123")) {
                        matchFlg = true;
                    }
                }

                if (!matchFlg) {
                    System.out.println("Double Byte Search Test 1 - Error");
                    errorFlg = true;
                }
            }

            okuyamaClient.setValueAndCreateIndex(this.nowCount + "test456", "漢字です。これから難しい試験を行います");
            String[] dByteChars2 = {"字","い"};
            dByteRet = okuyamaClient.searchValue(dByteChars2, "1");
            if(((String)dByteRet[0]).equals("true"))  {
                String[] retkeys = (String[])dByteRet[1];
                boolean matchFlg = false;
                for (int i= 0; i < retkeys.length; i++) {
                    if (retkeys[i].equals(this.nowCount + "test456")) {
                        matchFlg = true;
                    }
                }

                if (!matchFlg) {
                    System.out.println("Double Byte Search Test 2 - Error");
                    errorFlg = true;
                }
            }


            String[] dByteChars3 = {"字","夏"};
            dByteRet = okuyamaClient.searchValue(dByteChars3, "1");
            if(!((String)dByteRet[0]).equals("false"))  {
                System.out.println("Double Byte Search Test 3 - Error");
                System.out.println(((String[])dByteRet[1])[0]);
                errorFlg = true;
            }


            String[] dByteChars4 = {"これから難","試験を行い"};
            dByteRet = okuyamaClient.searchValue(dByteChars4, "1");
            if(!((String)dByteRet[0]).equals("true"))  {
                System.out.println("Double Byte Search Test 4 - Error");
                errorFlg = true;
            }


            String[] dByteChars5 = {"これから簡単","試験を行い"};
            dByteRet = okuyamaClient.searchValue(dByteChars5, "1");
            if(!((String)dByteRet[0]).equals("false"))  {
                System.out.println("Double Byte Search Teßst 5 - Error");
                errorFlg = true;
            }            

            
            int normalSetCount = 0;
            for (int i = start; i < count; i++) {
                // データ登録

                if (!okuyamaClient.setValueAndCreateIndex(this.nowCount + "createindexKey_" + new Integer(i).toString(), i+ "abc" + this.nowCount + "createindexvalue_" + new Integer(i).toString())) {
                    System.out.println("setValueAndCreateIndex - Error=[" + this.nowCount + "createindexKey_" + new Integer(i).toString()+"]");
                    errorFlg = true;
                }
                normalSetCount++;
                if ((i % 10000) == 0) System.out.println(i);
                if ((i - start) > 100) break;
            }

            // Prefixあり
            int prefixSetCount = 0;
            String prefix = "Pre" + this.nowCount + "fix";
            for (int i = start; i < count; i++) {
                // データ登録

                if (!okuyamaClient.setValueAndCreateIndex(this.nowCount + "createindexPrefixKey_" + new Integer(i).toString(), i+ "abc" + this.nowCount + "createindexvalue_" + new Integer(i).toString() , prefix)) {
                    System.out.println("setValueAndCreateIndex(Prefix) - Error=[" + this.nowCount + "createindexPrefixKey_" + new Integer(i).toString()+"]");
                    errorFlg = true;
                }
                prefixSetCount++;
                if ((i % 10000) == 0) System.out.println(i);
                if ((i - start) > 100) break;
            }


            // データ検索(単)
            for (int i = start; i < count; i++) {

                sChars = new String[1];
                sChars[0] = i + "abc" + this.nowCount + "create";
                searchRet = okuyamaClient.searchValue(sChars, "1");
                if (!searchRet[0].equals("true")) {
                    System.out.println("searchValue - 1-1 - Error=[" + sChars[0] + "]");
                    errorFlg = true;
                } else {
                    if (((String[])searchRet[1]).length != 1) {
                        String[] keys = (String[])searchRet[1];

                        Map multiGetRet = okuyamaClient.getMultiValue(keys);
                        for (int i2 = 0; i2 < keys.length; i2++) {
                            String val = (String)multiGetRet.get(keys[i2]);
                            if (val.indexOf(sChars[0]) == -1) {
                                System.out.println("searchValue - 1-2 - Error=[" + sChars[0] + "] Not Value[" + val + "]");
                                errorFlg = true;
                            }
                        }

                    } else if (!((String[])searchRet[1])[0].equals(this.nowCount + "createindexKey_" + new Integer(i).toString())) {
                        System.out.println("searchValue - 1-3 - Error=[" + sChars[0] + "]");
                        errorFlg = true;
                    }
                }
                if ((i - start) > 100) break;
            }


            // データ検索(複数)
            sChars = new String[1];
            sChars[0] = "abc" + this.nowCount + "create";

            searchRet = okuyamaClient.searchValue(sChars, "1");
            if (!searchRet[0].equals("true")) {
                System.out.println("searchValue- 2-1 - Error=[" + sChars[0] + "]");
                errorFlg = true;
            } else {
                if (((String[])searchRet[1]).length != normalSetCount) {
                    System.out.println("searchValue - 2-2 - rror=[" + sChars[0] + "] Length=[" + ((String[])searchRet[1]).length + "] TrueCount=[" + normalSetCount + "]");
                    errorFlg = true;
                }
            }


            // データ検索(単)
            for (int i = start; i < count; i++) {

                sChars = new String[1];
                sChars[0] = i + "abc" + this.nowCount + "create";
                searchRet = okuyamaClient.searchValue(sChars, "1", prefix);
                if (!searchRet[0].equals("true")) {
                    System.out.println("searchValue(Prefix) - 1-1 - Error=[" + sChars[0] + "]");
                    errorFlg = true;
                } else {
                    if (((String[])searchRet[1]).length != 1) {

                        String[] keys = (String[])searchRet[1];

                        Map multiGetRet = okuyamaClient.getMultiValue(keys);
                        for (int i2 = 0; i2 < keys.length; i2++) {
                            String val = (String)multiGetRet.get(keys[i2]);
                            if (val.indexOf(sChars[0]) == -1) {
                                System.out.println("searchValue(Prefix) - 1-2 - Error=[" + sChars[0] + "] Not Value[" + val + "]");
                                errorFlg = true;
                            }
                        }
                    } else if (!((String[])searchRet[1])[0].equals(this.nowCount + "createindexPrefixKey_" + new Integer(i).toString())) {
                        System.out.println("searchValue(Prefix) - 1-3 - Error=[" + sChars[0] + "]");
                        errorFlg = true;
                    }
                }
                if ((i - start) > 100) break;
            }


            // データ検索(複数)
            sChars = new String[1];
            sChars[0] = "abc" + this.nowCount + "create";

            searchRet = okuyamaClient.searchValue(sChars, "1", prefix);
            if (!searchRet[0].equals("true")) {
                System.out.println("searchValue(Prefix)- 2-1 - Error=[" + sChars[0] + "]");
                errorFlg = true;
            } else {
                if (((String[])searchRet[1]).length != prefixSetCount) {
                    System.out.println("searchValue(Prefix) - 2-2 - Error=[" + sChars[0] + "] Length=[" + ((String[])searchRet[1]).length + "] TrueCount=[" + prefixSetCount + "]");
                    errorFlg = true;
                }
            }

            // Index削除
            for (int i = start; i < count; i++) {

                if (!okuyamaClient.removeSearchIndex(this.nowCount + "createindexKey_" + new Integer(i).toString())) {
                    System.out.println("removeValue- 3-1 - Error=[" + this.nowCount + "createindexKey_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } 
                if ((i - start) > 100) break;
            }

            sChars = new String[1];
            sChars[0] =  "abc" + this.nowCount + "create";

            searchRet = okuyamaClient.searchValue(sChars, "1");
            if (!searchRet[0].equals("false")) {
                System.out.println("removeValue- 4-1 - Error=[" + sChars[0] + "]");
                errorFlg = true;
            }


            // Index削除(Prefix)
            for (int i = start; i < count; i++) {

                if (!okuyamaClient.removeSearchIndex(this.nowCount + "createindexPrefixKey_" + new Integer(i).toString(), prefix)) {
                    System.out.println("removeValue(Prefix)- 3-1 - Error=[" + this.nowCount + "createindexPrefixKey_" + new Integer(i).toString() + "]");
                    errorFlg = true;
                } 
                if ((i - start) > 100) break;
            }

            sChars = new String[1];
            sChars[0] = "abc" + this.nowCount + "create";

            searchRet = okuyamaClient.searchValue(sChars, "1", prefix);
            if (!searchRet[0].equals("false")) {
                System.out.println("removeValue(Prefix) - 4-1 - Error=[" + sChars[0] + "]");
                errorFlg = true;
            }



            long endTime = new Date().getTime();
            System.out.println("CreateIndex & searchValue & removeIndex Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execIndex - End");

        return errorFlg;
    }

    private boolean execSetExpireAndGet(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execSetExpireAndGet - Start");
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
            // データ登録 - 1
            if (!okuyamaClient.setValue(this.nowCount + "expiredatasavekey_" + new Integer(start).toString(), this.nowCount + "setexpiretestdata123456789_" + new Integer(start).toString(), new Integer(10))) {
                System.out.println("SetExpireAndGet - Set Error=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + ", " + this.nowCount + "setexpiretestdata123456789_" + new Integer(start).toString());
                errorFlg = true;
            }

            if (!okuyamaClient.setValue(this.nowCount + "expiredatasavekey_" + new Integer(start+1).toString(), this.nowCount + "setexpiretestdata123456789_" + new Integer(start+1).toString(), new Integer(3))) {
                System.out.println("SetExpireAndGet - Set Error=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+1).toString() + ", " + this.nowCount + "setexpiretestdata123456789_" + new Integer(start+1).toString());
                errorFlg = true;
            }

            if (!okuyamaClient.setValue(this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString(), this.nowCount + "setexpiretestdata123456789_" + new Integer(start+2).toString(), new Integer(8))) {
                System.out.println("SetExpireAndGet - Set Error=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString() + ", " + this.nowCount + "setexpiretestdata123456789_" + new Integer(start+2).toString());
                errorFlg = true;
            }

            Thread.sleep(5000);
            
            String[] ret = okuyamaClient.getValue(this.nowCount + "expiredatasavekey_" + new Integer(start).toString());

            if (ret[0].equals("true")) {

            } else if (ret[0].equals("false")) {
                logger.error("データなし - 1 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error - 1 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getValue(this.nowCount + "expiredatasavekey_" + new Integer(start+1).toString());

            if (ret[0].equals("true")) {
                logger.error("データあり - 2 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+1).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("false")) {

            } else if (ret[0].equals("error")) {
                System.out.println("Error - 2 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getValueAndUpdateExpireTime(this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString());

            if (ret[0].equals("true")) {

            } else if (ret[0].equals("false")) {
                logger.error("データなし - 3 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error - 3 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString() + "]" + ret[1]);
                errorFlg = true;
            }

            Thread.sleep(5500);

            ret = okuyamaClient.getValue(this.nowCount + "expiredatasavekey_" + new Integer(start).toString());

            if (ret[0].equals("true")) {

                logger.error("データあり - 1 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("false")) {

            } else if (ret[0].equals("error")) {
                System.out.println("Error - 1 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getValue(this.nowCount + "expiredatasavekey_" + new Integer(start+1).toString());

            if (ret[0].equals("true")) {
                logger.error("データあり - 2 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+1).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("false")) {

            } else if (ret[0].equals("error")) {
                System.out.println("Error - 2 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getValueAndUpdateExpireTime(this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString());

            if (ret[0].equals("true")) {

            } else if (ret[0].equals("false")) {
                logger.error("データなし - 3 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error - 3 Key=[" + this.nowCount + "expiredatasavekey_" + new Integer(start+2).toString() + "]" + ret[1]);
                errorFlg = true;
            }

            long endTime = new Date().getTime();
            System.out.println("SetExpireAndGet Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execSetExpireAndGet - End");

        return errorFlg;
    }


    private boolean execGetMultiTagValues(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execGetMultiTagValues - Start");

            long startTime = new Date().getTime();
            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] tag1 = {start+"_" + this.nowCount + "_tag1_m"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1_m",start+"_" + this.nowCount + "_tag2_m"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1_m",start+"_" + this.nowCount + "_tag2_m",start+"_" + this.nowCount + "_tag3_m"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4_m"};
            String[] tag5 = {start+"_" + this.nowCount + "_tag4_m", start+"_" + this.nowCount + "_tag1_m"};
            String[] setTag = null;

            ArrayList tag1RetList = new ArrayList();
            ArrayList tag2RetList = new ArrayList();
            ArrayList tag3RetList = new ArrayList();
            ArrayList tag4RetList = new ArrayList();
            int counter = 0;
            for (int i = 0; i < 100; i++) {

                if (counter == 0) {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_m_" + new Integer(i).toString() + "]", tag1, "tagsampledatakey_m_" + new Integer(i).toString() + "]");
                    counter++;
                } else if (counter == 1) {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_m_" + new Integer(i).toString() + "]", tag2, "tagsampledatakey_m_" + new Integer(i).toString() + "]");
                    counter++;
                } else if (counter == 2) {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_m_" + new Integer(i).toString() + "]", tag3, "tagsampledatakey_m_" + new Integer(i).toString() + "]");
                    counter++;
                } else {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_m_" + new Integer(i).toString() + "]", tag4, "tagsampledatakey_m_" + new Integer(i).toString() + "]");
                    counter = 0;
                }
            }

            Map ret = null;

            ret = okuyamaClient.getMultiTagValues(tag1, true);
            if(ret.size() != 75) {
                System.out.println(start+"_" + this.nowCount + "_tag1_m - Error");
                errorFlg = true;
            }

            ret = okuyamaClient.getMultiTagValues(tag2, true);
            if(ret.size() != 50) {
                System.out.println(start+"_" + this.nowCount + "_tag1_m, AND _tag2_m  - Error");
                errorFlg = true;
            }

            ret = okuyamaClient.getMultiTagValues(tag3, true);
            if(ret.size() != 25) {
                System.out.println(start+"_" + this.nowCount + "_tag1_m, AND _tag2_m, AND _tag3_m  - Error");
                errorFlg = true;
            }

            ret = okuyamaClient.getMultiTagValues(tag5, false);
            if(ret.size() != 100) {
                System.out.println(start+"_" + this.nowCount + "_tag1_m, OR _tag4_m - Error");
                errorFlg = true;
            }

            long endTime = new Date().getTime();
            System.out.println("Tag Multi Get Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execGetMultiTagValues - End");
        return errorFlg;
    }


    private boolean execGetMultiTagKeys(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execGetMultiTagKeys - Start");

            long startTime = new Date().getTime();
            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }

            String[] tag1 = {start+"_" + this.nowCount + "_tag1_mk"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1_mk",start+"_" + this.nowCount + "_tag2_mk"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1_mk",start+"_" + this.nowCount + "_tag2_mk",start+"_" + this.nowCount + "_tag3_mk"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4_mk"};
            String[] tag5 = {start+"_" + this.nowCount + "_tag4_mk", start+"_" + this.nowCount + "_tag1_mk"};
            String[] setTag = null;

            ArrayList tag1RetList = new ArrayList();
            ArrayList tag2RetList = new ArrayList();
            ArrayList tag3RetList = new ArrayList();
            ArrayList tag4RetList = new ArrayList();
            int counter = 0;
            for (int i = 0; i < 100; i++) {

                if (counter == 0) {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_mk_" + new Integer(i).toString() + "]", tag1, "tagsampledatakey_mk_" + new Integer(i).toString() + "]");
                    counter++;
                } else if (counter == 1) {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_mk_" + new Integer(i).toString() + "]", tag2, "tagsampledatakey_mk_" + new Integer(i).toString() + "]");
                    counter++;
                } else if (counter == 2) {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_mk_" + new Integer(i).toString() + "]", tag3, "tagsampledatakey_mk_" + new Integer(i).toString() + "]");
                    counter++;
                } else {

                    okuyamaClient.setValue(this.nowCount + "tagsampledatakey_mk_" + new Integer(i).toString() + "]", tag4, "tagsampledatakey_mk_" + new Integer(i).toString() + "]");
                    counter = 0;
                }
            }

            String[] ret = null;

            ret = okuyamaClient.getMultiTagKeys(tag1, true);
            if(ret.length != 75) {
                System.out.println(start+"_" + this.nowCount + "_tag1_mk - Error");
                errorFlg = true;
            }

            ret = okuyamaClient.getMultiTagKeys(tag2, true);
            if(ret.length  != 50) {
                System.out.println(start+"_" + this.nowCount + "_tag1_mk, AND _tag2_mk  - Error");
                errorFlg = true;
            }

            ret = okuyamaClient.getMultiTagKeys(tag3, true);
            if(ret.length  != 25) {
                System.out.println(start+"_" + this.nowCount + "_tag1_mk, AND _tag2_mk, AND _tag3_mk  - Error");
                errorFlg = true;
            }

            ret = okuyamaClient.getMultiTagKeys(tag5, false);
            if(ret.length  != 100) {
                System.out.println(start+"_" + this.nowCount + "_tag1_mk, OR _tag4_mk - Error");
                errorFlg = true;
            }

            long endTime = new Date().getTime();
            System.out.println("Tag Multi Get Keys Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execGetMultiTagKeys - End");
        return errorFlg;
    }

    private boolean execObjectSetGet(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execObjectSetGet - Start");
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
            // データ登録 - 1
            if (!okuyamaClient.setObjectValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString(), this.nowCount + "setexpiretestdata123456789_" + new Integer(start).toString(), new Integer(10))) {
                System.out.println("SetExpireAndGet - Set Error=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + ", " + this.nowCount + "setexpiretestdata123456789_" + new Integer(start).toString());
                errorFlg = true;
            }

            if (!okuyamaClient.setObjectValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start+1).toString(), this.nowCount + "setexpiretestdata123456789_" + new Integer(start+1).toString(), new Integer(3))) {
                System.out.println("SetExpireAndGet - Set Error=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+1).toString() + ", " + this.nowCount + "setexpiretestdata123456789_" + new Integer(start+1).toString());
                errorFlg = true;
            }

            if (!okuyamaClient.setObjectValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString(), this.nowCount + "setexpiretestdata123456789_" + new Integer(start+2).toString(), new Integer(8))) {
                System.out.println("SetExpireAndGet - Set Error=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString() + ", " + this.nowCount + "setexpiretestdata123456789_" + new Integer(start+2).toString());
                errorFlg = true;
            }

            Thread.sleep(5000);
            
            Object[] ret = okuyamaClient.getObjectValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString());

            if (ret[0].equals("true")) {
                if(!((String)ret[1]).equals(this.nowCount + "setexpiretestdata123456789_" + new Integer(start).toString())) {
                    logger.error("データ間違い - 1 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]");
                    errorFlg = true;                    
                }
            } else if (ret[0].equals("false")) {
                logger.error("データなし - 1 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error - 1 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getObjectValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start+1).toString());

            if (ret[0].equals("true")) {
                logger.error("データあり - 2 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+1).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("false")) {

            } else if (ret[0].equals("error")) {
                System.out.println("Error - 2 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getObjectValueAndUpdateExpireTime(this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString());

            if (ret[0].equals("true")) {
                if(!((String)ret[1]).equals(this.nowCount + "setexpiretestdata123456789_" + new Integer(start+2).toString())) {
                    logger.error("データ間違い - 3 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString() + "]");
                    errorFlg = true;                    
                }
            } else if (ret[0].equals("false")) {
                logger.error("データなし - 3 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error - 3 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString() + "]" + ret[1]);
                errorFlg = true;
            }

            Thread.sleep(5500);

            ret = okuyamaClient.getValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString());

            if (ret[0].equals("true")) {

                logger.error("データあり - 1 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("false")) {

            } else if (ret[0].equals("error")) {
                System.out.println("Error - 1 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getValue(this.nowCount + "expireobjectdatasavekey_" + new Integer(start+1).toString());

            if (ret[0].equals("true")) {
                logger.error("データあり - 2 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+1).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("false")) {

            } else if (ret[0].equals("error")) {
                System.out.println("Error - 2 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start).toString() + "]" + ret[1]);
                errorFlg = true;
            }


            ret = okuyamaClient.getObjectValueAndUpdateExpireTime(this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString());

            if (ret[0].equals("true")) {

            } else if (ret[0].equals("false")) {
                logger.error("データなし - 3 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString() + "]");
                errorFlg = true;
            } else if (ret[0].equals("error")) {
                System.out.println("Error - 3 Key=[" + this.nowCount + "expireobjectdatasavekey_" + new Integer(start+2).toString() + "]" + ret[1]);
                errorFlg = true;
            }

            long endTime = new Date().getTime();
            System.out.println("SetObjectExpireAndObjectGet Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execObjectSetGet - End");

        return errorFlg;
    }


    private boolean execGetTagKeysResult(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        OkuyamaResultSet okuyamaResultSet = null;
        boolean errorFlg = false;
        try {
            System.out.println("execGetTagKeysResult - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }



            String[] tag1 = {start+"_" + this.nowCount + "_tag1_reslut"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1_reslut",start+"_" + this.nowCount + "_tag2_reslut"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1_reslut",start+"_" + this.nowCount + "_tag2_reslut",start+"_" + this.nowCount + "_tag3_reslut"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4_reslut"};
            String[] setTag = null;

            int counter = 0;
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

                if (!okuyamaClient.setValue(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString(), setTag, this.nowCount + "tagsamplesavedata_reslut_" + new Integer(i).toString())) {
                    System.out.println("Tag GetResult Set - Error=[" + this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString() + ", " + this.nowCount + "tagsamplesavedata_reslut_" + new Integer(i).toString());
                    errorFlg = true;
                }
            }


            ArrayList tag1RetList = new ArrayList();
            ArrayList tag2RetList = new ArrayList();
            ArrayList tag3RetList = new ArrayList();
            ArrayList tag4RetList = new ArrayList();
            counter = 0;
            for (int i = start; i < count; i++) {
                if (counter == 0) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 1) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    tag2RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 2) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    tag2RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    tag3RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 3) {

                    tag4RetList.add(this.nowCount + "tagsampledatakey_reslut_" + new Integer(i).toString());
                    counter = 0;
                }
            }

            HashMap getResult1 = new HashMap();
            HashMap getResult2 = new HashMap();
            HashMap getResult3 = new HashMap();
            HashMap getResult4 = new HashMap();
            String[] keys = null;
            long startTime = new Date().getTime();
            okuyamaResultSet = okuyamaClient.getTagKeysResult(start+"_" + this.nowCount + "_tag1_reslut");

            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    getResult1.put(okuyamaResultSet.getKey(), okuyamaResultSet.getValue());
                }
                okuyamaResultSet.close();
            } else {
                System.out.println(start+"tag1_reslut=データなし");
                errorFlg = true;
            }


            okuyamaResultSet = okuyamaClient.getTagKeysResult(start+"_" + this.nowCount + "_tag2_reslut");
            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    getResult2.put(okuyamaResultSet.getKey(), okuyamaResultSet.getValue());
                }
                okuyamaResultSet.close();
            } else {
                System.out.println(start+"tag2_reslut=データなし");
                errorFlg = true;
            }


            okuyamaResultSet = okuyamaClient.getTagKeysResult(start+"_" + this.nowCount + "_tag3_reslut");
            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    getResult3.put(okuyamaResultSet.getKey(), okuyamaResultSet.getValue());
                }
                okuyamaResultSet.close();
            } else {
                System.out.println(start+"tag3_reslut=データなし");
                errorFlg = true;
            }


            okuyamaResultSet = okuyamaClient.getTagKeysResult(start+"_" + this.nowCount + "_tag4_reslut");
            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    getResult4.put(okuyamaResultSet.getKey(), okuyamaResultSet.getValue());
                }
                okuyamaResultSet.close();
            } else {
                System.out.println(start+"tag4_reslut=データなし");
                errorFlg = true;
            }

            okuyamaResultSet = okuyamaClient.getTagKeysResult(start+"_" + this.nowCount + "_tag5_reslut");
            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    System.out.println(start+"tag5_reslut=データあり");
                    errorFlg = true;
                }
                okuyamaResultSet.close();
            } else {
                System.out.println(start+"tag5_reslut=OkuyamaResultSet取得できず");
                errorFlg = true;
            }



            // 検証
            // Tag1
            for (int idx = 0; idx < tag1RetList.size(); idx++) {
                if (!getResult1.containsKey((String)tag1RetList.get(idx))) {
                    System.out.println(start+"_tag1_result=該当データなし Key=[" + (String)tag1RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag2
            for (int idx = 0; idx < tag2RetList.size(); idx++) {
                if (!getResult2.containsKey((String)tag2RetList.get(idx))) {
                    System.out.println(start+"_tag2_result=該当データなし Key=[" + (String)tag2RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag3
            for (int idx = 0; idx < tag3RetList.size(); idx++) {
                if (!getResult3.containsKey((String)tag3RetList.get(idx))) {
                    System.out.println(start+"_tag3_result=該当データなし Key=[" + (String)tag3RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag4
            for (int idx = 0; idx < tag4RetList.size(); idx++) {
                if (!getResult4.containsKey((String)tag4RetList.get(idx))) {
                    System.out.println(start+"_tag4_result=該当データなし Key=[" + (String)tag4RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            long endTime = new Date().getTime();
            System.out.println("Tag Get Result Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execGetTagKeysResult - End");
        return errorFlg;
    }


    private boolean execMultiGetTagKeysResult(OkuyamaClient client, int start, int count) throws Exception {
        OkuyamaClient okuyamaClient = null;
        OkuyamaResultSet okuyamaResultSet = null;
        boolean errorFlg = false;
        try {
            System.out.println("execMultiGetTagKeysResult - Start");

            if (client != null) {
                okuyamaClient = client;
            } else {
                int port = masterNodePort;

                // クライアントインスタンスを作成
                okuyamaClient = new OkuyamaClient();

                // マスタサーバに接続
                okuyamaClient.connect(masterNodeName, port);
            }


            String[] tag1 = {start+"_" + this.nowCount + "_tag1_m_reslut"};
            String[] tag2 = {start+"_" + this.nowCount + "_tag1_m_reslut",start+"_" + this.nowCount + "_tag2_m_reslut"};
            String[] tag3 = {start+"_" + this.nowCount + "_tag1_m_reslut",start+"_" + this.nowCount + "_tag2_m_reslut",start+"_" + this.nowCount + "_tag3_m_reslut"};
            String[] tag4 = {start+"_" + this.nowCount + "_tag4_m_reslut"};
            String[] setTag = null;

            int counter = 0;
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

                if (!okuyamaClient.setValue(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString(), setTag, this.nowCount + "tagsamplesavedata_m_reslut_" + new Integer(i).toString())) {
                    System.out.println("Tag GetResult Set - Error=[" + this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString() + ", " + this.nowCount + "tagsamplesavedata_m_reslut_" + new Integer(i).toString());
                    errorFlg = true;
                }
            }


            ArrayList tag1RetList = new ArrayList();
            ArrayList tag2RetList = new ArrayList();
            counter = 0;
            for (int i = start; i < count; i++) {
                if (counter == 0) {

                    tag2RetList.add(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 1) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString());
                    tag2RetList.add(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 2) {

                    tag1RetList.add(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString());
                    tag2RetList.add(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString());
                    counter++;
                } else if (counter == 3) {

                    tag2RetList.add(this.nowCount + "tagsampledatakey_m_reslut_" + new Integer(i).toString());
                    counter = 0;
                }
            }

            HashMap getResult1 = new HashMap();
            HashMap getResult2 = new HashMap();
            HashMap getResult3 = new HashMap();
            HashMap getResult4 = new HashMap();
            String[] keys = null;
            long startTime = new Date().getTime();
            String[] andTestTag1 = {start+"_" + this.nowCount + "_tag1_m_reslut", start+"_" + this.nowCount + "_tag2_m_reslut"};
            okuyamaResultSet = okuyamaClient.getMultiTagKeysResult(andTestTag1);

            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    getResult1.put(okuyamaResultSet.getKey(), okuyamaResultSet.getValue());
                }
                okuyamaResultSet.close();
                if (getResult1.size() != tag1RetList.size()) {
                    System.out.println(start+"tag1&tag2_reslut=データ数異常");
                    errorFlg = true;
                }
            } else {
                System.out.println(start+"tag1&tag2_reslut=データなし");
                errorFlg = true;
            }


            String[] orTestTag2 = {start+"_" + this.nowCount + "_tag1_m_reslut", start+"_" + this.nowCount + "_tag2_m_reslut", start+"_" + this.nowCount + "_tag4_m_reslut"};
            okuyamaResultSet = okuyamaClient.getMultiTagKeysResult(orTestTag2, false);

            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    getResult2.put(okuyamaResultSet.getKey(), okuyamaResultSet.getValue());
                }
                okuyamaResultSet.close();
                if (getResult2.size() != tag2RetList.size()) {
                    System.out.println(start+"tag1&tag2&tag4_reslut=データ数異常");
                    errorFlg = true;
                }

            } else {
                System.out.println(start+"tag1&tag2&tag4_reslut=データなし");
                errorFlg = true;
            }



            String[] andTestTag3 = {start+"_" + this.nowCount + "_tag2_m_reslut", start+"_" + this.nowCount + "_tag3_m_reslut", start+"_" + this.nowCount + "_tag4_m_reslut"};
            okuyamaResultSet = okuyamaClient.getMultiTagKeysResult(andTestTag3, true);

            if (okuyamaResultSet != null) {
                // データ有り
                while (okuyamaResultSet.next()) {
                    errorFlg = true;
                    System.out.println(start+"tag2_tag3_tag4_reslut=データあり");
                    System.out.println(okuyamaResultSet.getKey());
                }
                okuyamaResultSet.close();
            }




            // 検証
            // Andの1番目
            for (int idx = 0; idx < tag1RetList.size(); idx++) {
                if (!getResult1.containsKey((String)tag1RetList.get(idx))) {
                    System.out.println(start+"AND_1=該当データなし Key=[" + (String)tag1RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }

            // Tag2
            // Orの1番目
            for (int idx = 0; idx < tag2RetList.size(); idx++) {
                if (!getResult2.containsKey((String)tag2RetList.get(idx))) {
                    System.out.println(start+"Or_1=該当データなし Key=[" + (String)tag2RetList.get(idx) +"]");
                    errorFlg = true;
                }
            }


            long endTime = new Date().getTime();
            System.out.println("Tag Get Result Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execMultiGetTagKeysResult - End");
        return errorFlg;
    }


    private boolean execList(OkuyamaClient client, int start, String listNamePrefix) throws Exception {
        OkuyamaClient okuyamaClient = null;
        boolean errorFlg = false;
        try {
            System.out.println("execList - Start");
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
            Random rnd = new  Random();
            String listName = listNamePrefix+"list"+System.nanoTime();
            // リスト構造作成
            String[] ret = okuyamaClient.createListStruct(listName);
            if (!ret[0].equals("true")) {
                System.out.println("List create error1 =" + listName);
                errorFlg = true;
            }
            ret = okuyamaClient.createListStruct(listName);
            if (!ret[0].equals("false")) {
                System.out.println("List create error2 =" + listName);
                errorFlg = true;
            }

            // LPUSH
            for (int idx = 0; idx < 5000; idx++) {
                String[] retTmp = okuyamaClient.listLPush(listName, "Llistdata."+idx);

                if (!retTmp[0].equals("true")) {
                
                    System.out.println("List create error3 =" + listName);
                    System.out.println(retTmp[1]);
                    errorFlg = true;
                }
            }
Object[] test = okuyamaClient.listLen(listName);
System.out.println(listName+" size=" + test[1]);
            // Indexテスト
            for (long idx = 0; idx < 5000L; idx++) {

                String[] retTmp = okuyamaClient.listIndex(listName, idx);
                if (!retTmp[0].equals("true")) {
                    System.out.println("List create error4 =" + listName);
                    System.out.println(retTmp[1]);
                    errorFlg = true;
                } else {
                    if (!retTmp[1].equals("Llistdata."+(4999-idx))) {
                        System.out.println("List create error4-1 =" + listName);
                        System.out.println(retTmp[1]);
                        errorFlg = true;
                    }
                }
            }

            // RPUSH
            for (int idx = 0; idx < 5000; idx++) {
                String[] retTmp = okuyamaClient.listRPush(listName, "Rlistdata."+idx);

                if (!retTmp[0].equals("true")) {
                
                    System.out.println("List create error5 =" + listName);
                    System.out.println(retTmp[1]);
                    errorFlg = true;
                }
            }
test = okuyamaClient.listLen(listName);
System.out.println(listName+" size=" + test[1]);

            // Indexテスト
            for (long idx = 0; idx < 5000L; idx++) {

                String[] retTmp = okuyamaClient.listIndex(listName, idx);
                if (!retTmp[0].equals("true")) {
                    System.out.println("List create error6 =" + listName);
                    System.out.println(retTmp[1]);
                    errorFlg = true;
                } else {
                    if (!retTmp[1].equals("Llistdata."+(4999-idx))) {
                        System.out.println("List create error6-1 =" + listName);
                        System.out.println(retTmp[1]);
                        errorFlg = true;
                    }
                }
            }
            // Indexテスト
            int idxTest2 = 0;
            for (long idx = 5000L; idx < 10000L; idx++) {
                
                String[] retTmp = okuyamaClient.listIndex(listName, idx);
                if (!retTmp[0].equals("true")) {
                    System.out.println("List create error7 =" + listName);
                    System.out.println(retTmp[1]);
                    errorFlg = true;
                } else {
                    if (!retTmp[1].equals("Rlistdata."+idxTest2)) {
                        System.out.println("List create error7-1 =" + listName);
                        System.out.println(retTmp[1]);
                        errorFlg = true;
                    }
                }
                idxTest2++;
            }



            long endTime = new Date().getTime();
            System.out.println("List Method= " + (endTime - startTime) + " milli second");

            if (client == null) {
                okuyamaClient.close();
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execList - End");

        return errorFlg;
    }
}