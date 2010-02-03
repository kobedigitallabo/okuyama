package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;

/**
 * MasterNodeのメイン実行部分<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractMasterManagerHelper extends AbstractHelper {

    protected static Hashtable checkErrorMap = new Hashtable(10);

    private int nowSt = 0;

    protected void execStart() {
        nowSt = 1;
        StatusUtil.addMgrExec();
    }

    protected void execEnd() {
        if(nowSt == 1) {
            nowSt = 0;
            StatusUtil.endMgrExec();
        }
    }
}