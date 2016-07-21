package okuyama.imdst.process;

import java.util.LinkedHashMap;
import java.util.Map;

import okuyama.base.lang.BatchDefine;
import okuyama.base.lang.BatchException;
import okuyama.base.process.IProcess;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.StatusUtil;

/**
 * okuyama用のPreProcess.<br>
 * 起動時の引数を解析し、反映する.<br>
 *
 * 起動オプション一覧<br>
 * -debug / デバッグモードで起動<br>
 * -c  MasterNodeの無操作コネクションタイムアウト時間(秒)<br>
 * -S  DataNodeのValueの保存可能最大サイズ(バイト)<br>
 * -s  DataNodeのValueの共通データファイルへの書き出し中間サイズ(バイト)(DataNode用設定ファイルのdataMemory=trueの場合のみ有効)<br>
 * -KS Keyの最大サイズ<br>
 * -v  分散モードがConsistentHash時(MasterNode用設定ファイルのDistributionAlgorithm=consistenthashの場合のみ)のVirtualNodeの数<br>
 * -fa ImdstDefine.parallelDiskAccess /ファイルシステムへの同時アクセス係数(整数)<br>
 * -ncot ImdstDefine.nodeConnectionOpenTimeout /DataNodeへのSocketコネクションOpenのタイムアウト閾値(ミリ秒)<br>
 * -nct ImdstDefine.nodeConnectionTimeout /DataNodeへのSocketコネクションreadのタイムアウト閾値(ミリ秒)<br>
 * -mmgrs ImdstDefine.maxMultiGetRequestSize /getMultiValueの際に一度にDataNodeに問い合わせるRequestKeyの数<br>
 * -sidc ImdstDefine.searchIndexDistributedCount /検索Indexを並列に作成する場合の並列数<br>
 * -gaetu ImdstDefine.getAndExpireTimeUpdate /データの有効期限をGetメソッドで更新するかの指定<br>
 * -fbmnk ImdstDefine.fileBaseMapNumberOfOneFileKey /FileBaseDataMapで1KeyファイルにどれだけのKey値を保存するかの指定<br>
 * -tlft  ImdstDefine.transactionLogFsyncType /WALログのファイルシステムへのfsync係数(0=OSでの自動sync制御、1=fsync回数低、2=fsync回数中、3=fsync回数高、4=常にfsync<br>
 * -vidf  ImdstDefine.vacuumInvalidDataFlg /有効期限切れのデータのクリーニングを行うかどうかの設定 true=行う false=行わない ※trueを指定するとファイルをストレージに使っている場合も実行される<br>
 * -svic  ImdstDefine.startVaccumInvalidCount /有効期限切れのデータのクリーニングを行う間隔(分/単位)<br>
 * -csf   ImdstDefine.calcSizeFlg /保存データの合計サイズを計算するかどうかの指定 true=計算する/false=計算しない 計算しない方が高速に値の登録が可能<br>
 * -rdvp ImdstDefine.reuseDataFileValuePositionFlg /完全ファイルモードでDataNodeを起動した際に値の更新時にデータファイル上のValueの場所を再利用するかの設定.true/再利用する, false/再利用しない<br>
 * -dwmqs ImdstDefine.delayWriteMaxQueueingSize /DelayWriteCoreFileBaseKeyMapのメモリ上へのキューイングレコード数<br>
 * -crcm ImdstDefine.compulsionRetryConnectMode /MasterNodeとDataNode間の処理に失敗した場合に強制的に1度だけ再処理を行うようにするかの設定 true/再接続する, false/再接続は自動<br>
 * -dcmuc ImdstDefine.datanodeConnectorMaxUseCount /MasterNodeとDataNode間のSockeの最大再利用回数 (整数) 少ない値にすると接続コストがかかる<br>
 * -smbsmf ImdstDefine.serializeMapBucketSizeMemoryFactor /SerializMapのBucketサイズのJVMへのメモリ割当に対する1Bucket当たりの係数(整数)<br>
 * -red ImdstDefine.recycleExsistData / 完全ファイルモード時に既に存在するデータを再利用する設定
 * -wfsrf ImdstDefine.workFileStartingReadFlg / DataNode起動時に操作記録ログ(トランザクションログ)を読み込む設定.trueの場合は読み込む(デフォルト)
 * -udt ImdstDefine.useDiskType / データファイルを保存するディスクのタイプを指定することで、ディスクへのアクセスが最適化される 1=HDD(デフォルト) 2=SSD
 * -mdcs ImdstDefine.maxDiskCacheSize / ディスクキャッシュ利用時に、どれだけの件数をキャッシュに乗せるかを件数で指定する デフォルトでは10000件
 * -efsmo ImdstDefine.executeFileStoreMapObject / バックアップ用のスナップショットObjectを出力するかの有無(デフォルト出力)
 * -lsdn ImdstDefine.lowSpecDataNode / DataNodeがLowSpecのサーバで稼働しているもしくはディスクが遅い、リカバリorノード追加時の負荷を下げたい場合にtrueとする
 * -lsdnsdc ImdstDefine.lowSpecDataNodeSendDataCount / DataNodeがリカバリ時、ノード追加時にデータを一度に転送する上限数を制御する
 * -scmn ImdstDefine.slaveClusterMasterNode / okuyamaをMasterNodeをマルチクラスターで起動する場合に当該ノードをスレーブノードとして起動する場合に、trueとする
 * -rocm ImdstDefine.rebuildOkuyamaClusterMode / okuyamaをMasterNodeをマルチクラスターで起動した場合にメインとなるクラスターがダウンし、Slaveからリビルドする場合にtrueとして起動
 * -npmmns ImdstDefine.notPromotionMainMasterNodeStatus / MainMasterNodeに昇格しないMasterNodeを作成する場合にtrueとする / このオプションはMasterNodeの中でもデータ復旧を行うMasterNodeを限定したい場合に使う。例えばスプリットブレインなどの現象でMasterNode同士が通信出来なくなった際に、それぞれのMasterNodeが勝手に復旧をしないためなどである。
 * -rr ImdstDefine.recoverRequired / DataNodeがリカバリが必要な場合にtrueとして起動する
 * -smnca ImdstDefine.solitaryMasterNodeCheckAddress / MasterNodeの孤立チェック用の到達確認先のアドレス文字列。icmpでの確認のため、確認先のアドレスのみをカンマ区切りで設定する。全てのアドレスに届かない場合に自動的にMasterNodeがshutdownする
 * -ncopt ImdstDefine.nodeConnectionOpenPingTimeout / MainMasterNodeがDataNodeの生存監視を行う際にコネクションオープン時のタイムアウト閾値時間 数値にて指定(単位はミリ秒)
 * -ncpt ImdstDefine.nodeConnectionPingTimeout / MainMasterNodeがDataNodeの生存監視を行う際に接続後Pingの応答を待機する閾値時間 数値にて指定(単位はミリ秒)
 * -vacsl  Key値の数とファイルの行数の差がこの数値を超えるとvacuumを行う候補となる
 * -vacscl Key値の数とファイルの行数の差がこの数値を超えると強制的にvacuumを行う
 * -vacat  Vacuum実行時に事前に以下のミリ秒の間アクセスがないと実行許可となる
 * -vac  Vacuumを行わない場合はfasle
 *
 * <br>
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerPreprocess implements IProcess {

    public String process(String option) throws BatchException {

        try {
            Map settingStartParameterMap = new LinkedHashMap();
            if (BatchDefine.USER_OPTION_STR != null) {
                String[] startOptions = BatchDefine.USER_OPTION_STR.split(" ");

                for (int i = 0; i < startOptions.length; i++) {

                    // -debug
                    if (startOptions[i].trim().toLowerCase().equals("-debug")) {
                        StatusUtil.setDebugOption(true);
                        settingStartParameterMap.put("-debug", "true");
                    }

                    // -cto MasterNodeコネクション無操作タイムアウト時間(単位は秒)
                    if (startOptions[i].trim().toLowerCase().equals("-c")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.masterNodeMaxConnectTime = Integer.parseInt(startOptions[i+1]) * 1000;
                                settingStartParameterMap.put("-c", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -S
                    if (startOptions[i].trim().equals("-S")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.saveDataMaxSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-S", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -s
                    if (startOptions[i].trim().equals("-s")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.dataFileWriteMaxSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-s", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -KS
                    if (startOptions[i].trim().equals("-KS")) {
                        if (startOptions.length > (i+1)) {
                            try {

                                ImdstDefine.saveKeyMaxSize = Integer.parseInt(startOptions[i+1]);
                                if (ImdstDefine.saveKeyMaxSize < 200) ImdstDefine.saveKeyMaxSize = 200;
                                settingStartParameterMap.put("-KS", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -ts
                    if (startOptions[i].trim().equals("-ts")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.tagValueAppendMaxSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-ts", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -v
                    if (startOptions[i].trim().equals("-v")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.consistentHashVirtualNode = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-v", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -fa
                    if (startOptions[i].trim().equals("-fa")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.parallelDiskAccess = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-fa", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -ncot
                    if (startOptions[i].trim().equals("-ncot")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.nodeConnectionOpenTimeout = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-ncot", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -nct
                    if (startOptions[i].trim().equals("-nct")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.nodeConnectionTimeout = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-nct", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -mmgrs
                    if (startOptions[i].trim().equals("-mmgrs")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.maxMultiGetRequestSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-mmgrs", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -sidc
                    if (startOptions[i].trim().equals("-sidc")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.searchIndexDistributedCount = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-sidc", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -gaetu
                    // TODO:未実装
                    if (startOptions[i].trim().equals("-gaetu")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.getAndExpireTimeUpdate = true;
                                settingStartParameterMap.put("-gaetu", "true");
                            }
                        }
                    }


                    // -fbmnk
                    if (startOptions[i].trim().equals("-fbmnk")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.fileBaseMapNumberOfOneFileKey = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-fbmnk", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -tlft
                    if (startOptions[i].trim().equals("-tlft")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.transactionLogFsyncType = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-tlft", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -vidf
                    if (startOptions[i].trim().equals("-vidf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.vacuumInvalidDataFlg = false;
                                settingStartParameterMap.put("-vidf", "false");
                            } else if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")){
                                ImdstDefine.vacuumInvalidDataCompulsion = true;
                                settingStartParameterMap.put("-vidf", "true");
                            }
                        }
                    }


                    // -svic
                    if (startOptions[i].trim().equals("-svic")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.startVaccumInvalidCount = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-svic", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -csf
                    if (startOptions[i].trim().equals("-csf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.calcSizeFlg = false;
                                settingStartParameterMap.put("-csf", "false");
                            } else if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")){
                                ImdstDefine.calcSizeFlg = true;
                                settingStartParameterMap.put("-csf", "true");
                            }
                        }
                    }


                    // -rdvp
                    if (startOptions[i].trim().equals("-rdvp")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.reuseDataFileValuePositionFlg = false;
                                settingStartParameterMap.put("-rdvp", "false");
                            } else if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")){
                                ImdstDefine.reuseDataFileValuePositionFlg = true;
                                settingStartParameterMap.put("-rdvp", "true");
                            }
                        }
                    }


                    // -dwmqs
                    if (startOptions[i].trim().equals("-dwmqs")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.delayWriteMaxQueueingSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-dwmqs", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -crcm
                    if (startOptions[i].trim().equals("-crcm")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.compulsionRetryConnectMode = true;
                                settingStartParameterMap.put("-crcm", "true");
                            }
                        }
                    }


                    // -dcmuc
                    if (startOptions[i].trim().equals("-dcmuc")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.datanodeConnectorMaxUseCount = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-dcmuc", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -smbsmf
                    if (startOptions[i].trim().equals("-smbsmf")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.serializeMapBucketSizeMemoryFactor = Long.parseLong(startOptions[i+1]);
                                settingStartParameterMap.put("-smbsmf", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -pcmf
                    if (startOptions[i].trim().equals("-pcmf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.pageCacheMappendFlg = true;
                                settingStartParameterMap.put("-pcmf", "true");
                            }
                        }
                    }

                    // -pcms
                    if (startOptions[i].trim().equals("-pcms")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.pageCacheMappendSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-pcms", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -red
                    if (startOptions[i].trim().equals("-red")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.recycleExsistData = false;
                                settingStartParameterMap.put("-red", "false");
                            }
                        }
                    }


                    // -wfsrf
                    if (startOptions[i].trim().equals("-wfsrf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.workFileStartingReadFlg = false;
                                settingStartParameterMap.put("-wfsrf", "false");
                            }
                        }
                    }

                    // -udt
                    if (startOptions[i].trim().equals("-udt")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                int type = Integer.parseInt(startOptions[i+1]);
                                if (type == 2) ImdstDefine.useDiskType = 2;
                                settingStartParameterMap.put("-udt", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -pcmf
                    if (startOptions[i].trim().equals("-pcmf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.pageCacheMappendFlg = true;
                                settingStartParameterMap.put("-pcmf", "true");
                            }
                        }
                    }


                    // -dfssf
                    if (startOptions[i].trim().equals("-dfssf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.dataFileSequentialSchedulingFlg = true;
                                settingStartParameterMap.put("-dfssf", "true");
                            }
                        }
                    }


                    // -mdcs
                    if (startOptions[i].trim().equals("-mdcs")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.maxDiskCacheSize = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-mdcs", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -efsmo
                    if (startOptions[i].trim().equals("-efsmo")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.executeFileStoreMapObject = false;
                                settingStartParameterMap.put("-efsmo", "false");
                            }
                        }
                    }

                    // -lsdn
                    if (startOptions[i].trim().equals("-lsdn")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.lowSpecDataNode = true;
                                settingStartParameterMap.put("-lsdn", "true");
                            }
                        }
                    }

                    // -lsdnsdc
                    if (startOptions[i].trim().equals("-lsdnsdc")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.lowSpecDataNodeSendDataCount = Integer.parseInt(startOptions[i+1]);
                                if (ImdstDefine.lowSpecDataNodeSendDataCount < 0) ImdstDefine.lowSpecDataNodeSendDataCount = 2000;
                                settingStartParameterMap.put("-lsdnsdc", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -scmn
                    if (startOptions[i].trim().equals("-scmn")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.slaveClusterMasterNode = true;
                                settingStartParameterMap.put("-scmn", "true");
                            }
                        }
                    }

                    // -rocm
                    if (startOptions[i].trim().equals("-rocm")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.rebuildOkuyamaClusterMode = true;
                                settingStartParameterMap.put("-rocm", "true");
                            }
                        }
                    }

                    // -npmmns
                    if (startOptions[i].trim().equals("-npmmns")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.notPromotionMainMasterNodeStatus = true;
                                settingStartParameterMap.put("-npmmns", "true");
                            }
                        }
                    }

                    // -rr
                    if (startOptions[i].trim().equals("-rr")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")) {
                                ImdstDefine.recoverRequired = true;
                                settingStartParameterMap.put("-rr", "true");
                            }
                        }
                    }

                    // -smnca
                    if (startOptions[i].trim().equals("-smnca")) {
                        if (startOptions.length > (i+1)) {
                            ImdstDefine.solitaryMasterNodeCheckAddress = startOptions[i+1].trim();
                            settingStartParameterMap.put("-smnca", startOptions[i+1].trim());
                        }
                    }

                    // -vacsl
                    if (startOptions[i].trim().equals("-vacsl")) {
                        if (startOptions.length > (i+1)) {
                            ImdstDefine.vacuumStartLimit = Integer.parseInt(startOptions[i+1]);
                            settingStartParameterMap.put("-vacsl", startOptions[i+1].trim());
                        }
                    }

                    // -vacscl
                    if (startOptions[i].trim().equals("-vacscl")) {
                        if (startOptions.length > (i+1)) {
                            ImdstDefine.vacuumStartCompulsionLimit = Integer.parseInt(startOptions[i+1]);
                            settingStartParameterMap.put("-vacscl", startOptions[i+1].trim());
                        }
                    }

                    // -vacat
                    if (startOptions[i].trim().equals("-vacat")) {
                        if (startOptions.length > (i+1)) {
                            ImdstDefine.vacuumExecAfterAccessTime = Integer.parseInt(startOptions[i+1]);
                            settingStartParameterMap.put("-vacat", startOptions[i+1].trim());
                        }
                    }

                    // -vac
                    if (startOptions[i].trim().equals("-vac")) {
                        if (startOptions.length > (i+1)) {
                            ImdstDefine.vacuumExec = Boolean.parseBoolean(startOptions[i+1]);
                            settingStartParameterMap.put("-vac", startOptions[i+1].trim());
                        }
                    }

                    // -vacbig
                    if (startOptions[i].trim().equals("-vacbig")) {
                        if (startOptions.length > (i+1)) {
                            ImdstDefine.bigDataVacuum = Boolean.parseBoolean(startOptions[i+1]);
                            settingStartParameterMap.put("-vacbig", startOptions[i+1].trim());
                        }
                    }

                    // -ncopt
/*                    if (startOptions[i].trim().equals("-ncopt")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.nodeConnectionOpenPingTimeout = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-ncopt", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -ncpt
                    if (startOptions[i].trim().equals("-ncpt")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.nodeConnectionPingTimeout = Integer.parseInt(startOptions[i+1]);
                                settingStartParameterMap.put("-ncpt", startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }*/
                }
            }

            System.out.println("Boot arguments");
            if (settingStartParameterMap.size() > 0) {
                System.out.println(" " +settingStartParameterMap);
            } else {
                System.out.println(" {}");
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }

        return "success";
    }
}