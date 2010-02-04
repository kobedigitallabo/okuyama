package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;

/**
 * MasterNodeのメイン実行部分<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractMasterManagerHelper extends AbstractHelper {

    protected static Hashtable checkErrorMap = new Hashtable(10);

    protected static Hashtable tmpSavePool = new Hashtable();

    private static Object tmpSync = new Object();

    private int nowSt = 0;

    protected void setTmpSaveData(String nodeInfo, String[] values) {
        synchronized (tmpSync) {
            ArrayList tmpList = null;
            if (tmpSavePool.containsKey(nodeInfo)) {
                tmpList = (ArrayList)tmpSavePool.get(nodeInfo);
            } else {
                tmpList = new ArrayList();
            }

            tmpList.add(values);
            tmpSavePool.put(nodeInfo, tmpList);
        }
    }

    /**
     * 一時保存データを取得する
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    protected ArrayList getTmpSaveDataList(String nodeInfo) {
        ArrayList retList = new ArrayList();
        if (tmpSavePool.containsKey(nodeInfo)) {
            retList = (ArrayList)tmpSavePool.get(nodeInfo);
        }
        return retList;
    }


    /**
     * 一時保存データ削除する
     *
     *
     * @param nodeInfo 削除対象のノード情報
     */
    protected void removeTmpSaveData(String nodeInfo) {
        if (tmpSavePool.containsKey(nodeInfo)) {
            tmpSavePool.remove(nodeInfo);
        }
    }


    /**
     * ノードの生存を確認
     *
     *
     * @param nodeInfo 確認対象のノード情報
     */
    protected boolean isNodeArrival(String nodeInfo) {
        if (checkErrorMap.containsKey(nodeInfo)) return false;
        return true;
    }

    /**
     * ノードの復帰を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setArriveNode(String nodeInfo) {
        checkErrorMap.remove(nodeInfo);

    }

    /**
     * ノードの停止を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setDeadNode(String nodeInfo) {
        checkErrorMap.put(nodeInfo, new Boolean(false));

    }

    /**
     *ノードの一時停止を要求.<br>
     *
     */
    protected void setNodeWaitStatus(String nodeInfo) {
        StatusUtil.setWaitStatus(nodeInfo);
    }

    /**
     *ノードの一時停止を解除.<br>
     *
     */
    protected void removeNodeWaitStatus(String nodeInfo) {
        StatusUtil.removeWaitStatus(nodeInfo);
    }


    /**
     * ノードに対するアクセスを開始をマーク
     *
     */
    protected void execNodeUseStart(String nodeInfo) {
        StatusUtil.addNodeUse(nodeInfo);
    }

    /**
     * ノードに対するアクセス終了をマーク
     *
     */
    protected void execNodeUseEnd(String nodeInfo) {
        StatusUtil.endNodeUse(nodeInfo);
    }

    /**
     * 指定ノードの使用状況を取得
     * 返却値は現在の使用数をあらわす
     *
     */
    protected int getNodeUseStatus(String nodeInfo) {
        return StatusUtil.getNodeUseStatus(nodeInfo);
    }

}