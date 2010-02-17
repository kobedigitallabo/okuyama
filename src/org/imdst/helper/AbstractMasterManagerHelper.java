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

    private static int connPoolCount = 0;

    private int nowSt = 0;

	private static HashMap allConnectionMap = new HashMap();

    private static Object connSync = new Object();

    /**
     * ノードの生存を確認
     *
     *
     * @param nodeInfo 確認対象のノード情報
     */
    protected boolean isNodeArrival(String nodeInfo) {
        return StatusUtil.isNodeArrival(nodeInfo);
    }

    /**
     * ノードの復帰を登録
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setArriveNode(String nodeInfo) {
        StatusUtil.setArriveNode(nodeInfo);
    }

    /**
     * ノードの停止を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setDeadNode(String nodeInfo) {
        StatusUtil.setDeadNode(nodeInfo);
    }

    /**
     * ノードとの接続プールが有効か確認
     *
     *
     * @param nodeInfo 対象のノード情報
     * @return boolean true:有効 false:無効
     */
    protected boolean checkConnectionEffective(String nodeInfo, Long time) {
        if (time == null) return true;
        Long rebootTime = StatusUtil.getNodeRebootTime(nodeInfo);

        if (rebootTime == null) return true;
        if (rebootTime.longValue() <= time.longValue()) return true;
        return false;
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

	/**
	 *
	 *
	 */
	protected void setActiveConnection(String connectionName, HashMap connectionMap) {
    	synchronized(connSync) {
			ArrayList connList = null;
			connList = (ArrayList)allConnectionMap.get(connectionName);

			if (connList == null) connList = new ArrayList();

			connList.add(connectionMap);
			allConnectionMap.put(connectionName, connList);
			connPoolCount++;
		}
	}


	/**
	 *
	 *
	 */
	protected HashMap getActiveConnection(String connectionName) {
		HashMap ret = null;
    	synchronized(connSync) {
			ArrayList connList = (ArrayList)allConnectionMap.get(connectionName);
			if (connList != null) {
				if(connList.size() > 0) {
					ret = (HashMap)connList.remove(0);
					connPoolCount--;
				}
			}
		}

		if (ret != null) {
			if(!this.checkConnectionEffective(connectionName, (Long)ret.get("time"))) ret = null;
		}
		return ret;
		
	}

	protected int getNowConnectionPoolCount() {
		return connPoolCount;
	}
}