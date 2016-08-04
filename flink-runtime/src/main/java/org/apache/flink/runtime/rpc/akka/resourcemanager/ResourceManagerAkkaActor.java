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

package org.apache.flink.runtime.rpc.akka.resourcemanager;

import akka.actor.ActorRef;
import akka.actor.Status;
import org.apache.flink.runtime.rpc.akka.RunnableAkkaActor;
import org.apache.flink.runtime.rpc.resourcemanager.RegistrationResponse;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.resourcemanager.SlotAssignment;
import org.apache.flink.runtime.rpc.akka.messages.RegisterJobMaster;
import org.apache.flink.runtime.rpc.akka.messages.RequestSlot;

public class ResourceManagerAkkaActor extends RunnableAkkaActor {
	private final ResourceManager resourceManager;

	public ResourceManagerAkkaActor(ResourceManager resourceManager) {
		this.resourceManager = resourceManager;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		final ActorRef sender = getSender();

		if (message instanceof RegisterJobMaster) {
			RegisterJobMaster registerJobMaster = (RegisterJobMaster) message;

			try {
				RegistrationResponse response = resourceManager.registerJobMaster(registerJobMaster.getJobMasterRegistration());
				sender.tell(new Status.Success(response), getSelf());
			} catch (Exception e) {
				sender.tell(new Status.Failure(e), getSelf());
			}
		} else if (message instanceof RequestSlot) {
			RequestSlot requestSlot = (RequestSlot) message;

			try {
				SlotAssignment response = resourceManager.requestSlot(requestSlot.getSlotRequest());
				sender.tell(new Status.Success(response), getSelf());
			} catch (Exception e) {
				sender.tell(new Status.Failure(e), getSelf());
			}
		} else {
			super.onReceive(message);
		}
	}
}
