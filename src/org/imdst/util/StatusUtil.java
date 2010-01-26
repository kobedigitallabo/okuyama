package org.imdst.util;

import java.util.*;
import java.io.*;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;

/**
 * システム全般の稼動ステータス管理モジュール.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class StatusUtil {
    // 0:正常 1:異常 2:終了 3:一時停止
    private static int status = 0;

    public static void setStatus(int status) {
        StatusUtil.status = status;
    }

    public static int getStatus() {
        return StatusUtil.status;
    }

}