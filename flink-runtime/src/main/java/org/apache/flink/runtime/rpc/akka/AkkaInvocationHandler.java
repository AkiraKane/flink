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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.runtime.rpc.akka.messages.RpcMessage;
import org.apache.flink.runtime.rpc.akka.messages.VoidRpcMessage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class AkkaInvocationHandler implements InvocationHandler {
	private final ActorRef actorRef;
	private final Timeout timeout;

	public AkkaInvocationHandler(ActorRef actorRef, Timeout timeout) {
		this.actorRef = actorRef;
		this.timeout = timeout;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		System.out.println("Call " + method);

		if (method.getName().equals("getActorRef")) {
			return actorRef;
		} else if (method.getReturnType() != void.class) {
			RpcMessage rpcMessage = new RpcMessage(method.getName(), args);

			return Patterns.ask(actorRef, rpcMessage, timeout);
		} else {
			VoidRpcMessage rpcMessage = new VoidRpcMessage(method.getName(), args);
			actorRef.tell(rpcMessage, ActorRef.noSender());

			return null;
		}
	}
}
