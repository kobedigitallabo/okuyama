package okuyama.imdst.client;


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
    public static String QUEUE_TAKE_END_VALUE= "{OKUYAMA_QUEUE_END_DATA_MARKER}";


    /**
     * コンストラクタ
     *
     */
    public OkuyamaQueueClient() {
        super();
    }

    /**
     * Queue領域の作成.<br>
     *
     * @param queueName 作成Queue名
     * @retrun boolean 成否
     * @throw OkuyamaClientException
     */
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


    /**
     * Queueへの登録.<br>
     *
     * @param queueName 指定Queue名
     * @param data 登録データ
     * @retrun boolean 成否
     * @throw OkuyamaClientException
     */
    public boolean put(String queueName, String data) throws OkuyamaClientException {

        boolean ret = false;
        try {

            Object[] incrRet = super.incrValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_INDEX + queueName, 1L);
            if (incrRet[0].equals(new Boolean(true))) {

                if (((Long)incrRet[1]).longValue() > 0) {
                    String[] checkValue = super.getValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName + "_" + (((Long)incrRet[1]).longValue() - 1) + "_value");
                    if (checkValue[0].equals("false")) {
                        // 存在しない場合は、異常な状態のため消し込む。ただし新規性を保証して
                        super.setNewValue(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName + "_" + (((Long)incrRet[1]).longValue() - 1) + "_value", QUEUE_TAKE_END_VALUE);
                    }
                }

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


    /**
     * Queueから取得.<br>
     *
     * @param queueName 指定Queue名
     * @return 取得データ(指定時間以内に取得できない場合はnull)
     * @throw OkuyamaClientException
     */
    public String take(String queueName) throws OkuyamaClientException {
        return take(queueName, 1000 * 30);
    }


    /**
     * Queueから取得.<br>
     *
     * @param queueName 指定Queue名
     * @param timeOut 待ち受けタイムアウト時間(ミリ秒/単位)
     * @return 取得データ(指定時間以内に取得できない場合はnull)
     * @throw OkuyamaClientException
     */
    public String take(String queueName, long timeOut) throws OkuyamaClientException {

        String ret = null;
        boolean loopFlg = true;
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeOut;
        int maxContinueCount = 6;
        int nowContinueCount = 0;

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

                    // 取得データが終了データの場合は無視
                    if (queueValueRet[1].equals(QUEUE_TAKE_END_VALUE)) {

                        // 現在のIndexを取得した際に、Valueに終了を表す値がある場合は、以下のパターンが考えられる
                        // (1).現在値を登録している最中である。
                        // (2).take時にValueに終了を表す文字を入れた後に、QUEUE_NAME_PREFIX_NOW_POINTを更新する前にPGMがアボートした
                        // (1)の場合は時間的に解決されるが、(2)はなんだかの処置を行う必要がある
                        // TODO:ここで処置を行う
                        if (nowContinueCount > maxContinueCount) {
                            // (1)の可能性がなくなったので、(2)に対する処置を行う
                            String[] recoverPointRet = super.getValueVersionCheck(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName);
                            if (recoverPointRet[0].equals("true")) {
                                // リカバリ処置を行う場合は、まずバージョン込みでNOW_POINTを取得し、現在調べているNOW_POINTと比べる
                                // 比べた末同じ場合は、バージョンチェックしながら+1する
                                Long recoverPointLong = new Long(recoverPointRet[1]);
                                if (recoverPointLong.equals(queuePoint)) {
                                    // まだNOW_POINTが変わっていないので更新を試みる
                                    long recoverNowPoint = recoverPointLong.longValue();
                                    recoverNowPoint = recoverNowPoint + 1;
                                    // バージョンチェックをしながら更新
                                    super.setValueVersionCheck(QUEUE_NAME_PREFIX + QUEUE_NAME_PREFIX_NOW_POINT + queueName, new Long(recoverNowPoint).toString(), recoverPointRet[2]);
                                }
                            }
                            nowContinueCount = 0;
                        }
        
                        if (System.currentTimeMillis() > endTime) return null;
                        Thread.sleep(15);
                        nowContinueCount++;
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
