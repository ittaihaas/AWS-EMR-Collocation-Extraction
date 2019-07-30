package com.amazonaws.dsps192;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Main {

	public static void main(String[] args) {
		AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.defaultClient();
		String minPmi = args[1];
		String relMinPmi = args[2];

		HadoopJarStepConfig step0cfg = new HadoopJarStepConfig()
				.withJar("s3://dsps192assignment2/multiAWS.jar")
				.withMainClass("Step0")
				.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data")
				.withArgs("s3://dsps192assignment2/")
				.withArgs(minPmi)
				.withArgs(relMinPmi);

		StepConfig step0 = new StepConfig()
				.withName("Multi-Step")
				.withActionOnFailure("TERMINATE_JOB_FLOW")
				.withHadoopJarStep(step0cfg);

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(20)
				.withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
				.withHadoopVersion("2.8.5")
				.withEc2KeyName("assignment1Key")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-west-2a"));

		RunJobFlowRequest request = new RunJobFlowRequest()
				.withName("dsps192Assignment2")
				.withReleaseLabel("emr-5.3.1")
				.withSteps(step0)
				.withLogUri("s3://dsps192assignment2/logs")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withInstances(instances);

		RunJobFlowResult result = emr.runJobFlow(request);
		System.out.println("JobFlow id: " + result.getJobFlowId());
	}
}
