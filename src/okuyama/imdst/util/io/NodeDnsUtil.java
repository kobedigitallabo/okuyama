package okuyama.imdst.util.io;

import java.util.concurrent.ConcurrentHashMap;


/**
 * IPの名前解決を代行する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class NodeDnsUtil {

    private static ConcurrentHashMap dnsMap = null;

    private static boolean dnsEnable = false;

    private static Object sync = new Object();



    public NodeDnsUtil() {
    }


    public static void setNameMap(String name, String real) {
        synchronized (sync) {
            if (dnsMap == null) dnsMap = new ConcurrentHashMap(50, 48, 1024);
            dnsMap.put(name, real);
            dnsEnable = true;
        }
    }


    public static void removeNameMap(String name) {
        synchronized (sync) {
            if (dnsMap != null) {
                dnsMap.remove(name);
                if (dnsMap.size() == 0) {
                    dnsEnable = false;
                }
            }
        }
    }


    public static boolean isDnsEnable() {
        return dnsEnable;
    }


    public static String getNameToReal(String name) {
        if (dnsMap == null) return name;

        if (!dnsMap.containsKey(name)) return name;

        String ret = (String)dnsMap.get(name);

        if (ret == null) return name;
        return ret;
    }
}
