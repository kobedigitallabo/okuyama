package org.batch;

import org.batch.lang.BatchException;
import org.batch.parameter.config.JobConfig;
import org.batch.parameter.config.BatchConfig;

/**
 * JobControllerクラスのインタフェース.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IJobController {

    public void execute() throws BatchException;

}   