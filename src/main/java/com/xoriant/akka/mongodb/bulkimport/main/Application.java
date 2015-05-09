/*
#
# Copyright 2015 Xoriant Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0


#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 */

package com.xoriant.akka.mongodb.bulkimport.main;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;

import com.xoriant.akka.mongodb.bulkimport.actor.FileReaderActor;
import com.xoriant.akka.mongodb.bulkimport.actor.MongoInsertionActor;

public class Application {

	static final int INSERTION_ACTOR_POOL_SIZE = 2;
	static final int BATCH_SIZE = 100;

	public static void main(String[] args) throws Exception{

		ApplicationContext ctx = new GenericXmlApplicationContext(
				"springconfig.xml");

		final ActorSystem _system = ActorSystem.create("FileImportApp");

		final ActorRef mongoInsertionActor = _system.actorOf(Props.create(
				MongoInsertionActor.class, ctx).withRouter(
				new RoundRobinPool(INSERTION_ACTOR_POOL_SIZE)));

		final ActorRef fileReaderActor = _system.actorOf(Props.create(
				FileReaderActor.class, mongoInsertionActor, BATCH_SIZE));
		String path = Thread.currentThread().getContextClassLoader()
				.getResource("NameList.csv").getFile();
		fileReaderActor.tell(path, ActorRef.noSender());
		 Thread.sleep(5000);
	        _system.shutdown();
		
	}

}
