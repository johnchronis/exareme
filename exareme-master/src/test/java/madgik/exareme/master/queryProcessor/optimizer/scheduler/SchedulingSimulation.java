package madgik.exareme.master.queryProcessor.optimizer.scheduler;

import madgik.exareme.common.optimizer.FinancialProperties;
import madgik.exareme.common.optimizer.RunTimeParameters;
import madgik.exareme.master.app.cluster.ExaremeCluster;
import madgik.exareme.master.app.cluster.ExaremeClusterFactory;
import madgik.exareme.master.engine.executor.remote.operator.data.*;
import madgik.exareme.master.queryProcessor.graph.ConcreteQueryGraph;
import madgik.exareme.master.queryProcessor.graph.GraphGenerator;
import madgik.exareme.master.queryProcessor.optimizer.ContainerResources;
import madgik.exareme.master.queryProcessor.optimizer.MultiObjectiveQueryScheduler;
import madgik.exareme.master.queryProcessor.optimizer.SolutionSpace;
import madgik.exareme.master.queryProcessor.optimizer.assignedOperatorFilter.FastSubgraphFilter;
import madgik.exareme.master.queryProcessor.optimizer.containerFilter.NoContainerFilter;
import madgik.exareme.utils.check.Check;
import madgik.exareme.worker.art.executionEngine.ExecEngineConstants;
import madgik.exareme.worker.art.executionEngine.ExecutionEngineLocator;
import madgik.exareme.worker.art.executionEngine.ExecutionEngineProxy;
import madgik.exareme.worker.art.executionEngine.session.ExecutionEngineSession;
import madgik.exareme.worker.art.executionEngine.session.ExecutionEngineSessionPlan;
import madgik.exareme.worker.art.executionPlan.ExecutionPlan;
import madgik.exareme.worker.art.executionPlan.ExecutionPlanParser;
import madgik.exareme.worker.art.executionPlan.JsonBuilder;
import madgik.exareme.worker.art.executionPlan.parser.expression.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by johnchronis on 5/2/2016.
 */
public class SchedulingSimulation {

    PlanExpression planExpress;
    String SleepOperator= "madgik.exareme.master.engine.executor.remote.operator.sleepOperator";

    @Before
    public void init(){
        Logger.getRootLogger().setLevel(Level.DEBUG);
        planExpress = new PlanExpression();
        addPragmas(planExpress);

    }

    @Test
    public void testScheduling() throws Exception {
        addContainer("c1");
        addContainer("c2");
        addContainer("c3");
        addContainer("c4");

        addOperator("A",1,"c1",1,10);
        addOperator("B",1,"c2",1,10);
        addOperator("C",1,"c1",1,10);
        addOperator("D",1,"c2",1,10);
        addOperator("E",1,"c3",1,10);
        addOperator("F",1,"c4",1,10);
        addOperator("G",1,"c1",1,10);

        addOperatorLink("A","B");
        addOperatorLink("C","D");
        addOperatorLink("B","E");
        addOperatorLink("D","F");
        addOperatorLink("E","G");
        addOperatorLink("D","G");

        JsonBuilder jsonBuilder = new JsonBuilder();
        String jsonEP = jsonBuilder.parse(planExpress);
        System.out.println("JSON Plan:" + jsonEP);


        ExecutionPlanParser planParser = new ExecutionPlanParser();
        ExecutionPlan executionPlan = planParser.parse(jsonEP);
        System.out.println("Parsed :" + executionPlan.toString());

        System.out.println("------- TEST --------");
        ExaremeCluster miniCluster = ExaremeClusterFactory.createMiniCluster(1098, 8088, 5);
        miniCluster.start();
        System.out.println("Mini cluster started.");

        ExecutionEngineProxy engineProxy = ExecutionEngineLocator.getExecutionEngineProxy();
        ExecutionEngineSession engineSession = engineProxy.createSession();
        final ExecutionEngineSessionPlan sessionPlan = engineSession.startSession();
        sessionPlan.submitPlan(executionPlan);
        System.out.println("Submitted.");

        while (sessionPlan.getPlanSessionStatusManagerProxy().hasFinished() == false
                && sessionPlan.getPlanSessionStatusManagerProxy().hasError() == false) {
            Thread.sleep(100);
        }
        System.out.println("Exited");
        if (sessionPlan.getPlanSessionStatusManagerProxy().hasError() == true) {
            System.out.println(sessionPlan.getPlanSessionStatusManagerProxy().getErrorList().get(0));
        }





    }

    void addOperator(String name, int RunTime_SEC,String container, int parts, int sizeOfPart_MB){
        LinkedList<Parameter> params = new LinkedList<>();
        params.add(new Parameter("time",String.valueOf(RunTime_SEC)));
        params.add(new Parameter("outputName",name+"table"));
        params.add(new Parameter("parts",String.valueOf(parts)));
        params.add(new Parameter("sizeOfPart_MB",String.valueOf(sizeOfPart_MB)));
//        params.add(new Parameter("behavior","store_and_forward"));// activate if not inserted automatically
        String queryString = "S_"+String.valueOf(RunTime_SEC)+"_"+name;
        Operator operator = new Operator(name,SleepOperator,params,queryString,container,new HashMap<String, LinkedList<Parameter>>());
        planExpress.addOperator(operator);
    }


    void addContainer(String name){
        String MyIP = "195.134.66.19";
        String ip = MyIP+"_container_"+MyIP+"_" +String.valueOf(planExpress.getContainerList().size());
        int port=1099,dtport = planExpress.getContainerList().size()+8088;
        Container container = new Container(name,ip,port,dtport);
        planExpress.addContainer(container);
    }

    void addOperatorLink(String from,String to){
        String cont = null;
        for (Operator o: planExpress.getOperatorList()){
            if(o.operatorName == from){
                cont = o.containerName;
            }
        }
        if(cont==null){
            //BAM
        }
        LinkedList<Parameter> paramList = new LinkedList<>();//add fake partitions to transfer, make the operator produce them
        int i=0;
        for( OperatorLink oplink : planExpress.getOperatorLinkList()){
            if(oplink.from.equals(from)){
                ++i;
            }
        }
        paramList.add(new Parameter("table",from+"table"));
        paramList.add(new Parameter("part",String.valueOf(i)));

        OperatorLink opLink = new OperatorLink(from,to,cont,paramList);
        planExpress.addOperatorConnect(opLink);
    }

    void addPragmas(PlanExpression pe){
        pe.addPragma(
                new Pragma(ExecEngineConstants.PRAGMA_MATERIALIZED_BUFFER_READER,
                        MaterializedReader.class.getName()));
        pe.addPragma(
                new Pragma(ExecEngineConstants.PRAGMA_MATERIALIZED_BUFFER_WRITER,
                        MaterializedWriter.class.getName()));
        pe.addPragma(
                new Pragma(ExecEngineConstants.PRAGMA_INTER_CONTAINER_MEDIATOR_FROM,
                        InterContainerMediatorFrom.class.getName()));
        pe.addPragma(
                new Pragma(ExecEngineConstants.PRAGMA_INTER_CONTAINER_MEDIATOR_TO,
                        InterContainerMediatorTo.class.getName()));
        pe.addPragma(
                new Pragma(ExecEngineConstants.PRAGMA_INTER_CONTAINER_DATA_TRANSFER,
                        DataTransferRegister.class.getName()));
    }


}
