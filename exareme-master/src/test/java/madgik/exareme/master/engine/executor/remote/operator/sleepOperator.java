package madgik.exareme.master.engine.executor.remote.operator;

import madgik.exareme.worker.art.concreteOperator.AbstractMiMo;
import madgik.exareme.worker.art.executionPlan.entity.OperatorEntity;
import madgik.exareme.worker.art.executionPlan.parser.expression.Parameter;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by vagos on 16/4/2015.
 */
public class sleepOperator extends AbstractMiMo {
    private static final Logger log = Logger.getLogger(sleepOperator.class);

    @Override public void run() throws Exception {
        int sleepSeconds = Integer.parseInt(
            super.getParameterManager().getParameters().getParameter("time").get(0).getValue());
        log.info("Going to sleep for: " + sleepSeconds + " secs...");
        Thread.sleep(1000 * sleepSeconds);

        String sessionName=null;
        String filename = null;
        String FIP =null;
        String Fport = null;
        String Tport = null;
        String TIP = null;
        for (String outOp : super.getParameterManager().getOutOperators()) {
            for (Parameter param : super.getParameterManager().getOutOperatorParameters(outOp)) {
                log.info("#@$"+param.name+"|"+param.value);
            }

            log.info("## "+outOp+"|"+super.getParameterManager().getOutOperatorParameters(outOp).size());

//            log.debug("## SessionName: " + sessionName);
            File f = super.getDiskManager().getGlobalSession().requestAccess(outOp);

            log.debug("** "+f.getAbsolutePath());

// File sessionFile = super.getDiskManager().getGlobalSession().requestAccess(sessionName);
//            log.debug("SessionFile: " + sessionFile.getAbsolutePath());

        }



        log.info("Exiting...");
        exit(0);
    }
}
