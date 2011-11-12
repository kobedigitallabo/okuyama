package okuyama.imdst.process;

import okuyama.base.lang.BatchDefine;
import okuyama.base.lang.BatchException;
import okuyama.base.process.IProcess;

import okuyama.imdst.util.*;

/**
 * okuyama用のPreProcess.<br>
 * 起動時の引数を解析し、反映する.<br>
 *
 * 起動オプション一覧<br>
 * -debug / デバッグモードで起動<br>
 * -c  MasterNodeの無操作コネクションタイムアウト時間(秒)<br>
 * -S  DataNodeのValueの保存可能最大サイズ(バイト)<br>
 * -s  DataNodeのValueの共通データファイルへの書き出し中間サイズ(バイト)(DataNode用設定ファイルのdataMemory=trueの場合のみ有効)<br>
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
 * -rdvp ImdstDefine.reuseDataFileValuePositionFlg /完全ファイルモードでDataNodeを起動した際に値の更新時にデータファイル上のValueの場所を再利用するかの設定.true/再利用する, false/再利用しない
 *
 * <br>
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerPreprocess implements IProcess {

    public String process(String option) throws BatchException {

        try {
            if (BatchDefine.USER_OPTION_STR != null) {
                String[] startOptions = BatchDefine.USER_OPTION_STR.split(" ");

                for (int i = 0; i < startOptions.length; i++) {

                    // -debug
                    if (startOptions[i].trim().toLowerCase().equals("-debug")) StatusUtil.setDebugOption(true);

                    // -cto MasterNodeコネクション無操作タイムアウト時間(単位は秒)
                    if (startOptions[i].trim().toLowerCase().equals("-c")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.masterNodeMaxConnectTime = Integer.parseInt(startOptions[i+1]) * 1000;
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -S
                    if (startOptions[i].trim().equals("-S")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.saveDataMaxSize = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -s
                    if (startOptions[i].trim().equals("-s")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.dataFileWriteMaxSize = Integer.parseInt(startOptions[i+1]);
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
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -ts
                    if (startOptions[i].trim().equals("-ts")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.tagValueAppendMaxSize = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }


                    // -v
                    if (startOptions[i].trim().equals("-v")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.consistentHashVirtualNode = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -fa
                    if (startOptions[i].trim().equals("-fa")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.parallelDiskAccess = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -ncot
                    if (startOptions[i].trim().equals("-ncot")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.nodeConnectionOpenTimeout = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -nct
                    if (startOptions[i].trim().equals("-nct")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.nodeConnectionTimeout = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -mmgrs
                    if (startOptions[i].trim().equals("-mmgrs")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.maxMultiGetRequestSize = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -sidc
                    if (startOptions[i].trim().equals("-sidc")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.searchIndexDistributedCount = Integer.parseInt(startOptions[i+1]);
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
                            }
                        }
                    }

                    // -fbmnk
                    if (startOptions[i].trim().equals("-fbmnk")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.fileBaseMapNumberOfOneFileKey = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -tlft
                    if (startOptions[i].trim().equals("-tlft")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.transactionLogFsyncType = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }
   
                    // -vidf
                    if (startOptions[i].trim().equals("-vidf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.vacuumInvalidDataFlg = false;
                            } else if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")){
                                ImdstDefine.vacuumInvalidDataCompulsion = true;
                            }
                        }
                    }

                    // -svic
                    if (startOptions[i].trim().equals("-svic")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.startVaccumInvalidCount = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
                            }
                        }
                    }

                    // -csf
                    if (startOptions[i].trim().equals("-csf")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.calcSizeFlg = false;
                            } else if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")){
                                ImdstDefine.calcSizeFlg = true;
                            }
                        }
                    }

                    // -rdvp
                    if (startOptions[i].trim().equals("-rdvp")) {
                        if (startOptions.length > (i+1)) {
                            if (startOptions[i+1] != null && startOptions[i+1].trim().equals("false")) {
                                ImdstDefine.reuseDataFileValuePositionFlg = false;
                            } else if (startOptions[i+1] != null && startOptions[i+1].trim().equals("true")){
                                ImdstDefine.reuseDataFileValuePositionFlg = true;
                            }
                        }
                    }

                }
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }

        return "success";
    }
}