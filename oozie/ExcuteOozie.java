package com.boco.wangyou.test;


import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;


public class ExcuteOozie {
    private static final String HDFS_CONFIG_PATH = "config/hadoop_config.properties";
    private static final ConfigUtils configUtils = new ConfigUtils();
    private static final Properties hdfsProp = configUtils.getHdfsConfig(HDFS_CONFIG_PATH);
    private static final String masterHost = hdfsProp.getProperty("MASTER_HOST", "");
    private static final String nameNode = "hdfs://" + masterHost + ":8020";
    private static final String jobTracker = masterHost + ":8032";
    private static final String oozieHost = hdfsProp.getProperty("OOZIE_HOST", "");
    private static final String oozieNode = "http://" + oozieHost + ":11000/oozie/";

    private static final String CONFIG_PATH = "config/lte/ltemro_data.properties";
    private static final Properties prop = configUtils.getHdfsConfig(CONFIG_PATH);
    private static final String userPath = prop.getProperty("HDFS_USER_PATH", "");
    private static final String libPath = nameNode + userPath + prop.getProperty("LIB_PATH", "");
    private static final String applicationPath = nameNode + userPath + prop.getProperty("APPLICATION_PATH", "");
    private static final String applicationMrePath = nameNode + userPath + prop.getProperty("APPLICATION_PATH_MRE", "");

    private static final String HIVE_CONFIG_PATH = "config/hive.properties";
    private static final Properties hiveProp = configUtils.getHdfsConfig(HIVE_CONFIG_PATH);
    private static final String thriftHost = hiveProp.getProperty("THRIFT_HOST");

    private OozieClient wc = null;

    public ExcuteOozie(){
        wc = new OozieClient(oozieNode);
    }


    private String StartJob(List<WorkflowParameter> wfParameters)
            throws OozieClientException {
        Properties conf = wc.createConfiguration();
        conf.setProperty("nameNode", nameNode);
        conf.setProperty("jobTracker", jobTracker);
        conf.setProperty("queueName", "default");
        conf.setProperty("thriftHost", thriftHost);
        conf.setProperty(OozieClient.LIBPATH, libPath);

        conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
        conf.setProperty(OozieClient.APP_PATH, applicationPath);


        // setting workflow parameters
        if ((wfParameters != null) && (wfParameters.size() > 0)) {
            for (WorkflowParameter parameter : wfParameters){
                conf.setProperty(parameter.getName(), parameter.getValue());
            }
        }

        // submit and start the workflow job
        return wc.run(conf);
    }

    public WorkflowJob.Status GetJobStatus(String jobID) throws OozieClientException{
        WorkflowJob job = wc.getJobInfo(jobID);
        return job.getStatus();
    }

    public String Excute(){
        // Create parameters
        List<WorkflowParameter> wfParameters = new LinkedList<WorkflowParameter>();

        WorkflowParameter data_type = new WorkflowParameter("data_type","mro");
        wfParameters.add(data_type);

        // Start Oozing
        String jobId = "";

        try {
            jobId = StartJob(wfParameters);
            System.out.println("Workflow job running:");
            int times = 0;
            while(true){
                WorkflowJob.Status status = GetJobStatus(jobId);
                if(status == WorkflowJob.Status.RUNNING){
                    System.out.print("*");
                }else if(status == WorkflowJob.Status.SUCCEEDED){
                    System.out.println("Workflow job Complete !");
                    break;
                }else{
                    System.out.println("Problem starting Workflow job : " + status);
                    break;
                }
                times ++;
                if(times % 100 == 0) System.out.println();
                Thread.sleep(10000);
            }
        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return jobId;
    }

    class WorkflowParameter{
        String name;
        String value;

        WorkflowParameter(String name,String value){
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public String getValue() {
            return value;
        }
        public void setValue(String value) {
            this.value = value;
        }

    }
}
