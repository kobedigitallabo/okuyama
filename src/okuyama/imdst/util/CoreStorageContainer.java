package okuyama.imdst.util;

import java.util.*;
import java.io.*;


/**
 * okuyamaのメモリ上のオブジェクトを書き出す際に利用する専用コンテナ<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreStorageContainer implements Cloneable, Serializable {

    public long storeTime = 0L;

    public ICoreStorage storeObject = null;

    public Map dataSizeMap = null;

}