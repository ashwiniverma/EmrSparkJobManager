package com.mkapa.aws.emr.com.mkapa.aws.emr.model;

import java.util.List;

public class SparkConfig {

    private String awsAccessKey;
    private String awsSecretKey;
    private String clusterName;
    private String region;
    private String bucket;
    private List<JobConfig> jobs;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<JobConfig> getJobs() {
        return jobs;
    }

    public void setJobs(List<JobConfig> jobs) {
        this.jobs = jobs;
    }
}
