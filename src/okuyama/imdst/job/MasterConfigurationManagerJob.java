package okuyama.imdst.job;

import java.io.*;
import java.net.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;

import okuyama.imdst.client.ImdstKeyValueClient;

/**
 * MasterNodeの設定関係を管理するJob<br>
 * 主に設定ファイルの初期読み込み及び、データノード上に存在する設定情報を監視して<br>
 * 変更があった場合は取り込む.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterConfigurationManagerJob extends AbstractJob implements IJob {

    private String keyMapNodesStr = null;
    private String subKeyMapNodesStr = null;
    private String ruleStrProp = null;

    private String loadBalanceStr = null;

    // トランザクションノードの設定
    private String transactionModeStr = null;
    private String transactionManagerStr = null;

    // マスターノードの設定
    // 自身がMasterNodeかの判定情報(旧設定)
    private String mainMasterNodeModeStr = null;
    // Slaveマスターノードの接続情報(旧設定)
    private String slaveMasterNodeInfoStr = null;

    // 自身の情報
    private String myNodeInfoStr = null;
    // メインマスターノード接続情報
    private String mainMasterNodeInfoStr = null;
    // 全てのマスターノードの接続情報
    private String allMasterNodeInfoStr = null;

    // 分散方式 デフォルトはmode
    // 他にはconsistenthash
    private String dispatchMode = "mod";


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterConfigurationManagerJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("MasterConfigurationManagerJob - initJob - start");
        logger.debug("MasterConfigurationManagerJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("MasterConfigurationManagerJob - executeJob - start");
        String ret = SUCCESS;
        
        try{

            int helperCode = super.executeHelper("MasterConfigurationManagerHelper", null);

            Object[] helperRet = null;
            while(helperRet == null) {
                 helperRet = super.waitGetHelperReturnParam(helperCode, 10);
            }
        } catch(Exception e) {
            logger.error("MasterConfigurationManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("MasterConfigurationManagerJob - executeJob - end");
        return ret;
    }
}