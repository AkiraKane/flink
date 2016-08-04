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

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.dispatch.Mapper;
import akka.pattern.AskableActorSelection;
import akka.util.Timeout;
import org.apache.flink.runtime.rpc.RunnableRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import scala.concurrent.Future;

import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;

public class AkkaRpcService implements RpcService {
	private final ActorSystem actorSystem;
	private final Timeout timeout;
	private final Set<ActorRef> actors = new HashSet<>();

	public AkkaRpcService(ActorSystem actorSystem, Timeout timeout) {
		this.actorSystem = actorSystem;
		this.timeout = timeout;
	}

	@Override
	public <C extends RpcGateway> Future<C> connect(String address, final Class<C> clazz) {
		ActorSelection actorSel = actorSystem.actorSelection(address);

		AskableActorSelection asker = new AskableActorSelection(actorSel);

		Future<Object> identify = asker.ask(new Identify(42), timeout);

		return identify.map(new Mapper<Object, C>(){
			public C apply(Object obj) {
				ActorRef actorRef = ((ActorIdentity) obj).getRef();

				return (C) Proxy.newProxyInstance(
					ClassLoader.getSystemClassLoader(),
					new Class<?>[]{clazz},
					new AkkaInvocationHandler(actorRef, timeout));

			}
		}, actorSystem.dispatcher());
	}

	@Override
	public <S extends RpcServer<C>, C extends RpcGateway> C startServer(S methodHandler) {
		ActorRef ref;
		C self;

		ref = actorSystem.actorOf(
			Props.create(RpcAkkaActor.class, methodHandler)
		);

		self = (C) Proxy.newProxyInstance(
			ClassLoader.getSystemClassLoader(),
			new Class<?>[]{methodHandler.getSelfClass(), RunnableRpcGateway.class, AkkaGateway.class},
			new AkkaInvocationHandler(ref, timeout));

		actors.add(ref);

		return self;
	}

	@Override
	public <C extends RpcGateway> void stopServer(C selfGateway) {
		if (selfGateway instanceof AkkaGateway) {
			AkkaGateway akkaClient = (AkkaGateway) selfGateway;

			if (actors.contains(akkaClient.getActorRef())) {
				akkaClient.getActorRef().tell(PoisonPill.getInstance(), ActorRef.noSender());
			} else {
				// don't stop this actor since it was not started by this RPC service
			}
		}
	}

	@Override
	public void stopService() {
		actorSystem.shutdown();
		actorSystem.awaitTermination();
	}
}
