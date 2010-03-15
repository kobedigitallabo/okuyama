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
     * ノードの停止を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setDeadNode(String nodeInfo) {
        StatusUtil.setDeadNode(nodeInfo);
    }

    /**
     * ノードの復帰を登録
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setArriveNode(String nodeInfo) {
        StatusUtil.setArriveNode(nodeInfo);

		// MainのMasterNodeの場合のみ実行
		// SlaveのMasterNodeにもノードの復帰登録を依頼
		if (super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode) != null && 
				super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode).equals("true")) {

			// 対象のSlaveMasterNode全てに依頼
			String slaves = super.getPropertiesValue(ImdstDefine.Prop_SlaveMasterNodes);

			if (slaves != null && !slaves.trim().equals("")) {
				String[] slaveList = slaves.split(",");

				// 1ノードづつ実行
				for (int i = 0; i < slaveList.length; i++) {

					Socket socket = null;
					PrintWriter pw = null;
					BufferedReader br = null;

					try {

						// Slaveノード名とポートに分解
						String[] slaveNodeDt = slaveList[i].split(":");
				        socket = new Socket(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
	 
			            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
																										ImdstDefine.keyHelperClientParamEncoding)));
			            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
																						ImdstDefine.keyHelperClientParamEncoding));

			            // 文字列バッファ初期化
			            StringBuffer serverRequestBuf = new StringBuffer();

			            // 処理番号連結
			            serverRequestBuf.append("92");
			            // セパレータ連結
			            serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

			            // 停止ノード名連結
			            serverRequestBuf.append(nodeInfo);

			            // サーバ送信
			            pw.println(serverRequestBuf.toString());
			            pw.flush();

			            // サーバから結果受け取り
			            String serverRetStr = br.readLine();

			            String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

			            // 処理の妥当性確認
			            if (serverRet[0].equals("92")) {
			                if (!serverRet[1].equals("true")) {
								// TODO:復帰登録失敗
								// 異常事態だが、稼動していないことも考えられるので、
								// 無視する
								//System.out.println("Slave Master Node setArriveNode Error [" + slaveList[i] + "]");
							} 
						}
					} catch(Exception e) {

						// TODO:復帰登録失敗
						// 異常事態だが、稼動していないことも考えられるので、
						// 無視する
						//System.out.println("Slave Master Node setArriveNode Error [" + slaveList[i] + "]");
						//e.printStackTrace();
					} finally {
						try {
				            if (pw != null) {
				                // 接続切断を通知
				                pw.println(ImdstDefine.imdstConnectExitRequest);
				                pw.flush();

				                pw.close();
				                pw = null;
				            }

				            if (br != null) {
				                br.close();
				                br = null;
				            }

				            if (socket != null) {
				                socket.close();
				                socket = null;
				            }
						} catch(Exception e2) {
							// 無視
						}
					}
				}
			}
		}
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
     * @param nodeInfo 停止対象ノード
     */
    protected void setNodeWaitStatus(String nodeInfo) {
        StatusUtil.setWaitStatus(nodeInfo);

		// MainのMasterNodeの場合のみ実行
		// SlaveのMasterNodeにもノードの一時停止を依頼
		if (super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode) != null && 
				super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode).equals("true")) {

			// 対象のSlaveMasterNode全てに依頼
			String slaves = super.getPropertiesValue(ImdstDefine.Prop_SlaveMasterNodes);

			if (slaves != null && !slaves.trim().equals("")) {
				String[] slaveList = slaves.split(",");

				// 1ノードづつ実行
				for (int i = 0; i < slaveList.length; i++) {

					Socket socket = null;
					PrintWriter pw = null;
					BufferedReader br = null;

					try {

						// Slaveノード名とポートに分解
						String[] slaveNodeDt = slaveList[i].split(":");
				        socket = new Socket(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
	 
			            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
																										ImdstDefine.keyHelperClientParamEncoding)));
			            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
																						ImdstDefine.keyHelperClientParamEncoding));

			            // 文字列バッファ初期化
			            StringBuffer serverRequestBuf = new StringBuffer();

			            // 処理番号連結
			            serverRequestBuf.append("90");
			            // セパレータ連結
			            serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

			            // 停止ノード名連結
			            serverRequestBuf.append(nodeInfo);

			            // サーバ送信
			            pw.println(serverRequestBuf.toString());
			            pw.flush();

			            // サーバから結果受け取り
			            String serverRetStr = br.readLine();

			            String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

			            // 処理の妥当性確認
			            if (serverRet[0].equals("90")) {
			                if (!serverRet[1].equals("true")) {
								// TODO:停止失敗
								// 異常事態だが、稼動していないことも考えられるので、
								// 無視する
								//System.out.println("Slave Master Node setNodeWaitStatus Error [" + slaveList[i] + "]");
							} 
						}
					} catch(Exception e) {

						// TODO:停止失敗
						// 異常事態だが、稼動していないことも考えられるので、
						// 無視する
						//System.out.println("Slave Master Node setNodeWaitStatus Error [" + slaveList[i] + "]");
						//e.printStackTrace();
					} finally {
						try {
				            if (pw != null) {
				                // 接続切断を通知
				                pw.println(ImdstDefine.imdstConnectExitRequest);
				                pw.flush();

				                pw.close();
				                pw = null;
				            }

				            if (br != null) {
				                br.close();
				                br = null;
				            }

				            if (socket != null) {
				                socket.close();
				                socket = null;
				            }
						} catch(Exception e2) {
							// 無視
						}
					}
				}
			}
		}
    }


    /**
     *ノードの一時停止を解除.<br>
     *
     * @param nodeInfo 解除対象ノード
     */
    protected void removeNodeWaitStatus(String nodeInfo) {
        StatusUtil.removeWaitStatus(nodeInfo);

		// MainのMasterNodeの場合のみ実行
		// SlaveのMasterNodeにもノードの一時停止解除を依頼
		if (super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode) != null && 
				super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode).equals("true")) {

			// 対象のSlaveMasterNode全てに依頼
			String slaves = super.getPropertiesValue(ImdstDefine.Prop_SlaveMasterNodes);

			if (slaves != null && !slaves.trim().equals("")) {
				String[] slaveList = slaves.split(",");

				// 1ノードづつ実行
				for (int i = 0; i < slaveList.length; i++) {

					Socket socket = null;
					PrintWriter pw = null;
					BufferedReader br = null;

					try {

						// Slaveノード名とポートに分解
						String[] slaveNodeDt = slaveList[i].split(":");
				        socket = new Socket(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
	 
			            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
																										ImdstDefine.keyHelperClientParamEncoding)));
			            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
																						ImdstDefine.keyHelperClientParamEncoding));

			            // 文字列バッファ初期化
			            StringBuffer serverRequestBuf = new StringBuffer();

			            // 処理番号連結
			            serverRequestBuf.append("91");
			            // セパレータ連結
			            serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

			            // 停止ノード名連結
			            serverRequestBuf.append(nodeInfo);

			            // サーバ送信
			            pw.println(serverRequestBuf.toString());
			            pw.flush();

			            // サーバから結果受け取り
			            String serverRetStr = br.readLine();

			            String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

			            // 処理の妥当性確認
			            if (serverRet[0].equals("91")) {
			                if (!serverRet[1].equals("true")) {
								// TODO:停止解除失敗
								// 異常事態だが、稼動していないことも考えられるので、
								// 無視する
								//System.out.println("Slave Master Node removeNodeWaitStatus Error [" + slaveList[i] + "]");
							} 
						}
					} catch(Exception e) {

						// TODO:停止解除失敗
						// 異常事態だが、稼動していないことも考えられるので、
						// 無視する
						//System.out.println("Slave Master Node removeNodeWaitStatus Error [" + slaveList[i] + "]");
						//e.printStackTrace();
					} finally {
						try {
				            if (pw != null) {
				                // 接続切断を通知
				                pw.println(ImdstDefine.imdstConnectExitRequest);
				                pw.flush();

				                pw.close();
				                pw = null;
				            }

				            if (br != null) {
				                br.close();
				                br = null;
				            }

				            if (socket != null) {
				                socket.close();
				                socket = null;
				            }
						} catch(Exception e2) {
							// 無視
						}
					}
				}
			}
		}
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