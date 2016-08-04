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

import akka.actor.UntypedActor;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.akka.messages.RpcMessage;
import org.apache.flink.runtime.rpc.akka.messages.VoidRpcMessage;

import java.lang.reflect.Method;

public class RpcAkkaActor<S extends RpcServer> extends UntypedActor {
	private final S rpcServer;

	public RpcAkkaActor(S rpcServer) {
		this.rpcServer = rpcServer;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof RpcMessage) {
			System.out.println("Receive " + ((RpcMessage) message).getMethodName());
			RpcMessage rpcMessage = (RpcMessage) message;

			String methodName = rpcMessage.getMethodName();
			Object[] methodArgs = rpcMessage.getMethodArgs();

			Class<?>[] parameterTypes = new Class<?>[methodArgs.length];

			for (int i = 0; i < methodArgs.length; i++) {
				parameterTypes[i] = methodArgs[i].getClass();
			}

			Method method = rpcServer.getClass().getDeclaredMethod(methodName, parameterTypes);

			getSender().tell(method.invoke(rpcServer, methodArgs), getSelf());
		} else if (message instanceof VoidRpcMessage) {
			System.out.println("Receive " + ((VoidRpcMessage) message).getMethodName());
			VoidRpcMessage rpcMessage = (VoidRpcMessage) message;

			String methodName = rpcMessage.getMethodName();
			Object[] methodArgs = rpcMessage.getMethodArgs();

			Class<?>[] parameterTypes = new Class<?>[methodArgs.length];

			for (int i = 0; i < methodArgs.length; i++) {
				parameterTypes[i] = methodArgs[i].getClass();
			}

			if (methodName.equals("runAsync")) {
				((Runnable) methodArgs[0]).run();
			} else {
				try {
					Method method = rpcServer.getClass().getDeclaredMethod(methodName, parameterTypes);
					method.invoke(rpcServer, methodArgs);
				} catch (Throwable e) {
					System.out.println(e);
					throw e;
				}
			}
		}
	}
}
