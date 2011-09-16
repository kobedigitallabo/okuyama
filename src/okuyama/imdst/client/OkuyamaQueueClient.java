package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

import okuyama.imdst.util.ImdstDefine;


/**
 * MasterNodeと通信を行うプログラムインターフェース<br>
 * okuyamaを利用してキュー機構を実現するClient<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaQueueClient extends OkuyamaClient {

    public static String QUEUE_NAME_PREFIX = "Okuyama_Queue_Space_Name_Prefix_";
    public static String QUEUE_NAME_PREFIX_NOW_INDEX = "_Now_Index_";
    public static String QUEUE_NAME_PREFIX_NOW_POINT = "_Now_Point_";
    public static String QUEUE_TAKE_END_VALUE= "OKUYAMA_QUEUE_TAKE_PXIUYUI98613_TYGHU_END_";

    /**
     * コンストラクタ
     *
     */
    public OkuyamaQueueClient() {
        super();
    }

    public boolean createQueueSpace(String queueName) throws OkuyamaClientException {
        boolean ret = true;
        try {

            String[] setValueRet = super.setNewValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_INDEX + queueName, "0");
            if (!setValueRet[0].equals("true")) return false;

            setValueRet = super.setNewValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName, "1");
            if (!setValueRet[0].equals("true")) return false;
        } catch (OkuyamaClientException oce) {
            throw oce;
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }
        return ret;
    }

    public boolean removeQueueSpace(String queueName) throws OkuyamaClientException {
        return false;
    }


    public boolean put(String queueName, String data) throws OkuyamaClientException {

        boolean ret = false;
        try {

            Object[] incrRet = super.incrValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_INDEX + queueName, 1L);
            if (incrRet[0].equals(new Boolean(true))) {

                ret = super.setValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName + "_" + (Long)incrRet[1] + "_value", data);
                if (ret == false) {
                    throw new OkuyamaClientException("Queue Data Put Error");
                }
            } else {

                ret = false;
            }
        } catch (OkuyamaClientException oce) {
            throw oce;
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }
        return ret;
    }



    public String take(String queueName) throws OkuyamaClientException {
        return take(queueName, 1000 * 30);
    }

    public String take(String queueName, long timeOut) throws OkuyamaClientException {

        String ret = null;
        boolean loopFlg = true;
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeOut;

        try {

            while(loopFlg) {

                // 現在取得対象の場所を調べる

                String[] takeQueuePoint = super.getValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName);
                if (!takeQueuePoint[0].equals("true")) return null;

                // 現在の位置を取得
                Long queuePoint = new Long((String)takeQueuePoint[1]);

                // 現在の位置でQueueの値を取得する
                // この際にgetValueVersionCheckで取得

                String[] queueValueRet = super.getValueVersionCheck(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName + "_" + queuePoint + "_value");

                // 値が取れた場合と取得できない場合で処理を分岐
                if (queueValueRet[0].equals("true")) {
                    if (queueValueRet[1].equals(QUEUE_TAKE_END_VALUE)) {

                        if (System.currentTimeMillis() > endTime) return null;
                        Thread.sleep(15);
                        continue;
                    }
                    // 取得した値の利用を確定するためにCASで更新を行う
                    String[] checkUpdateRet = super.setValueVersionCheck(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName + "_" + queuePoint + "_value", QUEUE_TAKE_END_VALUE, queueValueRet[2]);
                    if (checkUpdateRet[0].equals("true")) {

                        // 利用を確定
                        // Queueの取得ポイントを更新する
                        Object[] queueTakePointUpdateRet = super.incrValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName, 1);
                        if (!queueTakePointUpdateRet[0].equals(new Boolean(true))) throw new OkuyamaClientException("Queue Take Point Update Error");

                        ret = queueValueRet[1];
                        loopFlg = false;
                    } else {
                        // ここで更新失敗は別クライアントがデータを更新したので再度やり直し
                        if (System.currentTimeMillis() > endTime) return null;
                        Thread.sleep(15);
                    }
                } else {
                    if (System.currentTimeMillis() > endTime) return null;
                    Thread.sleep(15);
                }
            }
        } catch (OkuyamaClientException oce) {
            throw oce;
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }
        return ret;
    }
}
