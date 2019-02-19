package com.mkapa.aws.emr;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mkapa.aws.emr.com.mkapa.aws.emr.model.JobConfig;
import com.mkapa.aws.emr.com.mkapa.aws.emr.model.SparkConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EmrSparkJobManager {

    private static final String PROP_FILE_NAME="emr-properties.json";
    AmazonElasticMapReduce emrClient;
    Properties properties = null;
    AmazonElasticMapReduce emr = null;
    private static final Logger logger = Logger.getLogger(EmrSparkJobManager.class.getName());

    /*private void readProperties() {
        properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(PROP_FILE_NAME));
        } catch (Exception ex) {
            logger.info("Error loading config file: " + ex);
        }
    }*/

   /* private JSONObject readJsonPropertiesFile() {
        //InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROP_FILE_NAME););
        JSONParser parser = new JSONParser();
        JSONObject data = null;
        try {
            //Use JSONObject for simple JSON and JSONArray for array of JSON.
            data = (JSONObject) parser.parse(
                    new FileReader(PROP_FILE_NAME));//path to the JSON file.
        } catch(IOException | ParseException ex) {
            logger.log(Level.INFO, "Error reading config file", ex);
        }
        return data;
    }*/

    private SparkConfig readJsonPropertiesFile() {
        SparkConfig config = null;
        ObjectMapper mapper = new ObjectMapper();
        InputStream instream = this.getClass().getClassLoader().getResourceAsStream(PROP_FILE_NAME);
        try {
            config = mapper.readValue(instream, SparkConfig.class);
        } catch(IOException ex) {
            logger.log(Level.INFO, "Error reading config file", ex);
        }
        return config;
    }
    public void submitJob() {

        SparkConfig config = readJsonPropertiesFile();
        emrClient = getEmrClient();
        StepFactory stepFactory = new StepFactory();

        StepConfig hive = new StepConfig("Hive", new StepFactory().newInstallHiveStep());

        String s3bucket = config.getBucket();
        List<JobConfig> jobs = config.getJobs();
        for (JobConfig jconfig : jobs) {
            String s3url = "s3://" + s3bucket + "/" + jconfig.getJarName();
            logger.info("s3url: " + s3url);
            HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig()
                    .withJar(s3url)
                    .withMainClass(jconfig.getClassName()) // optional main class, this can be omitted if jar above has a manifest
                    .withArgs("--verbose");

            StepConfig customStep = new StepConfig("Step1", hadoopConfig);

            AddJobFlowStepsResult result = emrClient.addJobFlowSteps(new AddJobFlowStepsRequest()
                    .withJobFlowId("j-1HTE8WKS7SODR")
                    .withSteps(hive, customStep));

            logger.info("stepids: " + result.getStepIds());
        }

    }

    private AmazonElasticMapReduce getEmrClient() {
        if (emr == null) {
            AWSCredentials awsCreds = new BasicAWSCredentials(properties.getProperty("awsAccessKey"),
                    properties.getProperty("awsSecretKey"));
            emr = AmazonElasticMapReduceClient.builder()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion(properties.getProperty("region")).build();
        }
        return emr;
    }

    public String getJobStatus(String jobFlowId) {
        String jobStatus = null;
        ListStepsRequest stepRequest = new ListStepsRequest().withClusterId(jobFlowId);
        ListStepsResult steps =getEmrClient().listSteps(stepRequest);
        StepSummary step = steps.getSteps().get(0);
        jobStatus = step.getStatus().getState();
        return jobStatus;
    }

}
