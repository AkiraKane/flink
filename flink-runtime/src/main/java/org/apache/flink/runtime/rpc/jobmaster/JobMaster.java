/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.jobmaster;

import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.resourcemanager.JobMasterRegistration;
import org.apache.flink.runtime.rpc.resourcemanager.RegistrationResponse;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;

import java.util.concurrent.ExecutorService;

public class JobMaster extends RpcServer<JobMasterGateway> {
	private final Logger LOG = LoggerFactory.getLogger(JobMaster.class);
	private final ExecutionContext executionContext;

	private ResourceManagerGateway resourceManager = null;

	public JobMaster(RpcService rpcService, ExecutorService executorService) {
		super(rpcService);
		executionContext = ExecutionContext$.MODULE$.fromExecutor(executorService);
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	@RpcMethod
	public void triggerResourceManagerRegistration(final String address) {
		Future<ResourceManagerGateway> resourceManagerFuture = getRpcService().connect(address, ResourceManagerGateway.class);

		Future<RegistrationResponse> registrationResponseFuture = resourceManagerFuture.flatMap(new Mapper<ResourceManagerGateway, Future<RegistrationResponse>>() {
			@Override
			public Future<RegistrationResponse> apply(final ResourceManagerGateway resourceManagerGateway) {
				runAsync(new Runnable() {
					@Override
					public void run() {
						resourceManager = resourceManagerGateway;
					}
				});

 				return resourceManagerGateway.registerJobMaster(new JobMasterRegistration());
			}
		}, executionContext);

		(registrationResponseFuture).onComplete(new OnComplete<RegistrationResponse>() {
			@Override
			public void onComplete(Throwable failure, RegistrationResponse success) throws Throwable {
				if (failure != null) {
					LOG.error("Registration at resource manager " + resourceManager + " failed. Tyr again.", failure);
				} else {
					getSelf().handleRegistrationResponse(success);
				}
			}
		}, executionContext);
	}

	@RpcMethod
	public void handleRegistrationResponse(RegistrationResponse response) {
		System.out.println("Received registration response: " + response);
		this.resourceManager = resourceManager;
	}

	public boolean isConnected() {
		return resourceManager != null;
	}

	@Override
	public Class<JobMasterGateway> getSelfClass() {
		return JobMasterGateway.class;
	}
}
