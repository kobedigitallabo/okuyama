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
 * -v  分散モードがConsistentHash時(MasterNode用設定ファイルのDistributionAlgorithm=consistenthashの場合のみ)のVirtualNodeの数
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

                    // -v
                    if (startOptions[i].trim().equals("-v")) {
                        if (startOptions.length > (i+1)) {
                            try {
                                ImdstDefine.consistentHashVirtualNode = Integer.parseInt(startOptions[i+1]);
                            } catch(NumberFormatException nfe) {
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