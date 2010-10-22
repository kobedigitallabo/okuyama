package okuyama.imdst.util;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;

/**
 * システム系のApiに対してアクセスする.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class JavaSystemApi {

    private static int cacheUseMemoryPercent = 2;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(JavaSystemApi.class);

    // 取得したCPU数をキャッシュしておく
    private static int availableProcessorsCache = 0;


    // gcを実行するかの閾値「全割り当てメモリ量中の何パーセント使用しているか」
    // この値を超えると、gcを発行する
    private static final int useMemSizeLimit = 95;


    /**
     * 自動でgcを発行する.<br>
     * 閾値による制限が自動で実行される.<br>
     */
    public static void autoGc() {
        if (JavaSystemApi.getUseMemoryPercent() >= useMemSizeLimit) {
            JavaSystemApi.gc();
        }
    }

    /**
     * 手動でgcを発行する.<br>
     */
    public static void manualGc() {
        JavaSystemApi.gc();
    }

    /**
     * gcの実行
     */
    private static void gc() {
        logger.info("JavaSystemApi - GC実行開始");
        System.gc();
        logger.info("JavaSystemApi - GC実行終了");
    }

    public static long getRuntimeMaxMem() {
        return getRuntimeMaxMem("");
    }

    /**
     * 現在のJVMに割り当ててあるMaxMemoryを取得する
     * @param format フォーマット指定 ""=バイト, "K"=キロバイト, "M"=メガバイト, "G"=ギガバイト
     * @return long
     */
    public static long getRuntimeMaxMem(String format) {
        long formatSize = getFormatLong(format);

        Runtime runtime = Runtime.getRuntime();
        return (runtime.maxMemory() / formatSize);
    }

    public static long getRuntimeTotalMem() {
        return getRuntimeTotalMem("");
    }

    /**
     * 現在のJVMに割り当ててあるTotalMemoryを取得する
     * @param format フォーマット指定 getRuntimeMaxMemと同様
     * @return 
     */
    public static long getRuntimeTotalMem(String format) {
        long formatSize = getFormatLong(format);

        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() / formatSize);
    }

    public static long getRuntimeFreeMem() {
        return getRuntimeFreeMem("");
    }

    /**
     * 現在のJVMに割り当ててあるFreeMemoryを取得する
     * @param format フォーマット指定 getRuntimeMaxMemと同様
     * @return 
     */
    public static long getRuntimeFreeMem(String format) {
        long formatSize = getFormatLong(format);

        Runtime runtime = Runtime.getRuntime();
        return (runtime.freeMemory() / formatSize);
    }


    // 現在使用しているメモリのパーセンテージを返す
    public static int getUseMemoryPercent() {
        double useMem = (getRuntimeTotalMem() - getRuntimeFreeMem());
        double usePercent = useMem / getRuntimeMaxMem();
        double percentSize = 100;
        usePercent = usePercent * percentSize;
        Double useMemSize = new Double(usePercent);

        cacheUseMemoryPercent = useMemSize.intValue();
        return cacheUseMemoryPercent;
    }

    public static int getUseMemoryPercentCache() {
        return cacheUseMemoryPercent;
    }

    // CPUの数を返す
    public static int getAvailableProcessors() {

        if (availableProcessorsCache == 0) {
            Runtime runtime = Runtime.getRuntime();
            availableProcessorsCache = runtime.availableProcessors();
        }

        return availableProcessorsCache;
    }


    // フォーマット用long値作成
    private static long getFormatLong(String format) {
        long formatSize = 1;

        if(format != null && !format.equals("")) {
            if(format.equals("K")) formatSize = 1000;
            if(format.equals("M")) formatSize = 1000000;
            if(format.equals("G")) formatSize = 1000000000;
        }
        return formatSize;
    }
}
