package fuse.okuyamafs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreMapFactory {


    public static int factoryType = 1;

    private static Map parameterMap = new ConcurrentHashMap();


    public static void init(int factoryType, String[] args) {

        CoreMapFactory.factoryType = factoryType;

        if (CoreMapFactory.factoryType == 1) {

            // NativeMap
            // No process
            parameterMap.put("okuyamainfo", args);
        } else {
            // OkuyamaFs
            parameterMap.put("okuyamainfo", args);
        }
    }

    public static IFsMap createDataMap() {
        if (CoreMapFactory.factoryType == 1) {
            return new NativeFsMap(1, (String[])parameterMap.get("okuyamainfo"));
        } else if(CoreMapFactory.factoryType == 2) {
//               return new NativeFsMap(1);
            return new OkuyamaFsMap(1, (String[])parameterMap.get("okuyamainfo"));
        } else if (CoreMapFactory.factoryType == 3) {
            return new LocalCacheOkuyamaFsMap(1, (String[])parameterMap.get("okuyamainfo"));
        }
        return null;
    }


    public static IFsMap createInfoMap() {
        if (CoreMapFactory.factoryType == 1) {
            return new NativeFsMap(2, (String[])parameterMap.get("okuyamainfo"));
        } else if(CoreMapFactory.factoryType == 2) {

            //return new NativeFsMap(2, (String[])parameterMap.get("okuyamainfo"));
            return new OkuyamaFsMap(2, (String[])parameterMap.get("okuyamainfo"));
        } else if (CoreMapFactory.factoryType == 3) {
            return new LocalCacheOkuyamaFsMap(2, (String[])parameterMap.get("okuyamainfo"));
        }
        return null;
    }


    public static IFsMap createDirMap() {
        if (CoreMapFactory.factoryType == 1) {
            return new NativeFsMap(3, (String[])parameterMap.get("okuyamainfo"));
        } else if(CoreMapFactory.factoryType == 2) {
            return new OkuyamaFsMap(3, (String[])parameterMap.get("okuyamainfo"));
        } else if (CoreMapFactory.factoryType == 3) {
            return new LocalCacheOkuyamaFsMap(3, (String[])parameterMap.get("okuyamainfo"));
        }
        return null;
    }
}
