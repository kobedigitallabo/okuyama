package org.imdst.job;

import java.io.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractJob;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.StatusUtil;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;


/**
 * MasterNode、自身でポートを上げて待ち受ける<br>
 * クライアントからの要求をHelperに依頼する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterManagerJob extends AbstractJob implements IJob {

    private int portNo = 5554;

    // サーバーソケット
    ServerSocket serverSocket = null;

    // 過去ルール
    private int[] oldRules = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterManagerJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("MasterManagerJob - initJob - start");

        this.portNo = Integer.parseInt(initValue);

        logger.debug("MasterManagerJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("MasterManagerJob - executeJob - start");
        String ret = SUCCESS;
        Object[] helperParams = null;
        try{

            // DataNodeと、KeyMapNodeの情報を初期化

            // DataNodeは本ノードと、レプリケーションノードの並べでUserParameterで
            // 設定されている想定
            // 形式は以下
            // DataNodeInfo=192.168.1.1:6060,192.168.1.2:6060;192.168.1.3:6080
            // DataSubNodeInfo=192.168.2.1:6060,192.168.2.2:6060;192.168.2.3:6080
            //
            // KeyMapNodeはEvenNodeとOddNodeで個別にUserParameterで
            // 設定されている想定
            // 形式は以下
            // KeyMapNode=192.168.3.1:7000,192.168.4.1:7000
            // KeyMapNodesRule=2 or 9 or 99 or 999
            this.parseAllNodesInfo();

            // サーバソケットの生成
            this.serverSocket = new ServerSocket(this.portNo);
            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);

            Socket socket = null;

            while (true) {
                if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) break;
                try {

                    // クライアントからの接続待ち
                    logger.debug("MasterManagerJob - ServerSocket - Access Wait");
                    socket = serverSocket.accept();
                    logger.debug(socket.getInetAddress() + " ACCESS");
                    helperParams = new Object[2];
                    helperParams[0] = socket;
                    helperParams[1] = this.oldRules;

                    super.executeHelper("MasterManagerHelper", helperParams);
                } catch (Exception e) {
                    if (StatusUtil.getStatus() == 2) {
                        logger.info("MasterManagerJob - executeJob - ServerEnd");
                        break;
                    }
                    logger.error(e);
                }
            }
        } catch(Exception e) {
            logger.error("MasterManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("MasterManagerJob - executeJob - end");
        return ret;
    }

    /**
     * KeyMapNodes,DataNodesの情報をパースする<br>
     * <br>
     * 以下の要素を設定する.<br>
     * KeyMapNodesRule=ルール値(2,9,99,999)<br>
     * KeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * DataNodesInfo=Dataノードの設定(DataNodeName1:11111, DataNodeName2:22222, DataNodeName3:33333)<br>
     * DataSubNodesInfo=DataSubノードの設定(DataSubNodeName1:11111, DataSubNodeName2:22222, DataSubNodeName3:33333)<br>
     * <br>
     * 記述の決まり.<br>
     */
    private void parseAllNodesInfo() {
        String keyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesInfo);
        String nodeStr = super.getPropertiesValue(ImdstDefine.Prop_DataNodesInfo);
        String subNodeStr = super.getPropertiesValue(ImdstDefine.Prop_DataSubNodesInfo);
        
        String ruleStrProp = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesRule);
        // ノード追加によりルールが変更されている可能性があるのパース
        // ルールは最新ルールが先頭に来るように設定される想定なので、先頭文字列を取得
        String[] ruleStrs = ruleStrProp.split(",") ;
        // 過去ルールを自身に保存
        if (ruleStrs.length > 1) {
            this.oldRules = new int[ruleStrs.length - 1];
            for (int i = 1; i < ruleStrs.length; i++) {
                this.oldRules[i - 1] = new Integer(ruleStrs[i].trim()).intValue();
            }
        }

        // 初期化
        DataDispatcher.init(ruleStrs[0], keyMapNodesStr, nodeStr, subNodeStr);

    }

}