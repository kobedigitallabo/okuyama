package org.okuyama.base;

import org.okuyama.base.lang.BatchException;
import org.okuyama.base.parameter.config.JobConfig;
import org.okuyama.base.parameter.config.BatchConfig;

/**
 * JobControllerクラスのインタフェース.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IJobController {

    public void execute() throws BatchException;

}   