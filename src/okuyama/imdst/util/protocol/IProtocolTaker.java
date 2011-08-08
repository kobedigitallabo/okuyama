package okuyama.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.io.CustomReader;

/**
 * クライアントとのProtocolの差を保管する.<br>
 * 基本的な動きは、クライアントとの接続からリクエストを抽出し、<br>
 * 結果を返す.<br>
 * パース後の動きを支持するために以下のインターフェースを持つ<br>
 * nextExecution()
 * return 1=そのまま処理を続行
 * return 2=continue
 * return 3=接続切断
 * return 9=異常終了
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IProtocolTaker {

    public void init();

    public void setClientInfo(String clientInfo);

    public String takeRequestLine(CustomReader br, PrintWriter pw) throws Exception;

    public String takeRequestLine(BufferedReader br, PrintWriter pw) throws Exception;

    public String[] takeRequestLine4List(CustomReader br, PrintWriter pw) throws Exception;

    public String takeResponseLine(String[] retParams) throws Exception;

    public String takeResponseLine(String[] retParams, BufferedOutputStream bos) throws Exception;

    public int nextExecution();

    public boolean isMatchMethod();

}
