package org.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import org.imdst.util.ImdstDefine;

/**
 * クライアントとのProtocolの差を保管するProtocolTakerをインスタンス化して返す.<br>
 *
 */
public class ProtocolTakerFactory {

    /**
     * Protocol名を渡すとそれに合ったTakerを返す.<br>
     * 対応しているProtocolは以下.<br>
     * "okuyama"=標準のokuyama<br>
     * "memcache"=memcache<br>
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
        } else if (protocol.equals("okuyama")) {
            // okuyama
            taker = new OkuyamaProtocolTaker(); 
        } else if (protocol.equals("memcache")) {
            // memcache
            taker = new MemcacheProtocolTaker(); 
        } else if (protocol.equals("memcache_datanode")) {
            // memcache
            taker = new MemcacheProtocolTaker4Data(); 
        } else {
            throw new Exception("Protocol Error [" + protocol + "]");
        }

        return taker;
    }
}