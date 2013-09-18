package fuse.okuyamafs;

import java.util.*;

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IFsMap {

    public boolean putNewString(Object key, String value);

    public boolean putNewMap(Object key, Map value);

    public boolean putNewBytes(Object key, byte[] value);

    public Object putBytes(Object key, byte[] value);

    public Object putMap(Object key, Map value);

    public Object putString(Object key, String value);


    public String getString(Object key);

    public Map getMap(Object key);

    public byte[] getBytes(Object key);


    public Object remove(Object key);

    public boolean removeExistObject(Object key);


    public boolean containsKey(Object key);
}
