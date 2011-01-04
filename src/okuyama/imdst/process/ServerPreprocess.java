package okuyama.imdst.process;

import okuyama.base.lang.BatchDefine;
import okuyama.base.lang.BatchException;
import okuyama.base.process.IProcess;

import okuyama.imdst.util.*;

/**
 * okuyama用のPreProcess.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerPreprocess implements IProcess {

    public String process(String option) throws BatchException {

        try {
            if (BatchDefine.USER_OPTION_STR != null) {
                String[] startOptions = BatchDefine.USER_OPTION_STR.split(" ");

                for (int i = 0; i < startOptions.length; i++) {
                    if (startOptions[i].trim().toLowerCase().equals("-debug")) 
                        StatusUtil.setDebugOption(true);
                }
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }

        return "success";
    }
}