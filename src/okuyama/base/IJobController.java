package okuyama.base;

import okuyama.base.lang.BatchException;
import okuyama.base.parameter.config.JobConfig;
import okuyama.base.parameter.config.BatchConfig;

/**
 * JobControllerクラスのインタフェース.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IJobController {

    public void execute() throws BatchException;

}   