package org.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import org.imdst.util.ImdstDefine;

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
 */
public interface IProtocolTaker {


    public String takeRequestLine(BufferedReader br, PrintWriter pw) throws Exception;

    public String takeResponseLine(String[] retParams) throws Exception;

    public int nextExecution();

    public boolean isMatchMethod();
}
