package org.imdst.util;

import java.util.*;
import java.io.*;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;

/**
 * システム全般の稼動ステータス管理モジュール.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class StatusUtil {

    // 0:処理をしていない 1以上:処理中
    private static int masterManagerStatus = 0;

    private static HashMap nodeStatusMap = new HashMap();
    private static Object nodeSync = new Object();

    private static HashMap nodeExecMap = new HashMap();

    // 0:正常 1:異常 2:終了 3:一時停止
    private static int status = 0;

    public static void initNodeExecMap(String[] nodeInfos) {
        for (int i = 0; i < nodeInfos.length; i++) {
            nodeExecMap.put(nodeInfos[i], new Integer(0));
        }
    }

    public static void setStatus(int status) {
        StatusUtil.status = status;
    }

    public static int getStatus() {
        return StatusUtil.status;
    }

    public static boolean isWaitStatus(String nodeInfo) {
        if(nodeStatusMap.containsKey(nodeInfo)) return true;
        return false;
    }

    public static void setWaitStatus(String nodeInfo) {
        synchronized(nodeSync) {
            nodeStatusMap.put(nodeInfo, new Boolean(true));
        }
    }

    public static void removeWaitStatus(String nodeInfo) {
        synchronized(nodeSync) {
            nodeStatusMap.remove(nodeInfo);
        }
    }

    // 本メソッドを呼び出す場合は必ずinitNodeExecMapで初期化を行う必要がある
    public static void addNodeUse(String nodeInfo) {
        synchronized(nodeExecMap) {
            Integer cnt = (Integer)nodeExecMap.get(nodeInfo);
            cnt = cnt + 1;
            nodeExecMap.put(nodeInfo, cnt);
        }
    }

    // 本メソッドを呼び出す場合は必ずinitNodeExecMapで初期化を行う必要がある
    public static void endNodeUse(String nodeInfo) {
        synchronized(nodeExecMap) {
            Integer cnt = (Integer)nodeExecMap.get(nodeInfo);
            cnt = cnt - 1;
            nodeExecMap.put(nodeInfo, cnt);
        }
    }

    // 本メソッドを呼び出す場合は必ずinitNodeExecMapで初期化を行う必要がある
    public static int getNodeUseStatus(String nodeInfo) {
        synchronized(nodeExecMap) {
            return ((Integer)nodeExecMap.get(nodeInfo)).intValue();
        }
    }

}