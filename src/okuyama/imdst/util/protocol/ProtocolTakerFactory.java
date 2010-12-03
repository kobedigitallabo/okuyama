package okuyama.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.imdst.util.ImdstDefine;

/**
 * クライアントとのProtocolの差を保管するProtocolTakerをインスタンス化して返す.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ProtocolTakerFactory {

    /**
     * Protocol名を渡すとそれに合ったTakerを返す.<br>
     * 対応しているProtocolは以下.<br>
     * "okuyama"=標準のokuyama<br>
     * "memcache" or "memcached"=memcached<br>
     *
     * @param protocol プロトコル名
     * @return IProtocolTaker
     * @exception Exception プロトコルが存在しない場合
     */
    public static IProtocolTaker getProtocolTaker(String protocol) throws Exception {
        IProtocolTaker taker = null;

        // Protocol判定
        if (protocol == null || protocol.trim().equals("")) {

            throw new Exception("Protocol Error [" + protocol + "]");
        } else if (protocol.equals(ImdstDefine.okuyamaProtocol)) {

            // okuyama
            taker = new OkuyamaProtocolTaker(); 
        } else if (protocol.equals(ImdstDefine.memcacheProtocol) || 
                    protocol.equals(ImdstDefine.memcachedProtocol)) {

            // memcached
            taker = new MemcachedProtocolTaker(); 
        } else if (protocol.equals(ImdstDefine.memcache4datanodeProtocol) || 
                    protocol.equals(ImdstDefine.memcached4datanodeProtocol)) {

            // memcached
            taker = new MemcachedProtocolTaker4Data(); 
        } else if (protocol.equals(ImdstDefine.memcache4proxyProtocol) || 
                    protocol.equals(ImdstDefine.memcached4proxyProtocol)) {

            // memcached for Proxy
            taker = new MemcachedProtocolTaker4Proxy(); 
        } else {
            throw new Exception("Protocol Error [" + protocol + "]");
        }

        return taker;
    }
}