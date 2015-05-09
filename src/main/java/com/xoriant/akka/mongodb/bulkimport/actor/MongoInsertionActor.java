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
package com.xoriant.akka.mongodb.bulkimport.actor;

import java.util.List;

import org.springframework.context.ApplicationContext;

import akka.actor.UntypedActor;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.xoriant.akka.mongodb.bulkimport.message.BatchCompleteMsg;
import com.xoriant.akka.mongodb.bulkimport.message.CSVRecordBatchMsg;
import com.xoriant.akka.mongodb.bulkimport.message.EndOfFileMsg;

public class MongoInsertionActor extends UntypedActor {

	private ApplicationContext applicationContext;
	private MongoClient mongoClient;
	public MongoInsertionActor(ApplicationContext contextParam) {
		this.applicationContext = contextParam;
		mongoClient = (MongoClient)applicationContext.getBean("mongoClient");
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if(message instanceof CSVRecordBatchMsg){
			CSVRecordBatchMsg csvRecordBatch = (CSVRecordBatchMsg)message;
			System.out.println("InsertionActor : Batch no "+csvRecordBatch.getBatchNo()+" received ack");
			DB db = mongoClient.getDB("akka-bulkimport");
			DBCollection personColl = db.getCollection("persons");
			BulkWriteOperation builder = personColl.initializeUnorderedBulkOperation();
			List<BasicDBObject> persons = csvRecordBatch.getRecords();
			for (BasicDBObject personDBObject : persons) {
				if(validate(personDBObject)){
					builder.insert(personDBObject);
				}
			}
			BulkWriteResult result = builder.execute();
			BatchCompleteMsg batchComplete = new BatchCompleteMsg(csvRecordBatch.getBatchNo(),result.getInsertedCount());
			getSender().tell(batchComplete, getSelf());
		}
		else if(message instanceof EndOfFileMsg){
			System.out.println("InsertionActor: EOF received");
		}
		else{
			unhandled(message);
		}
	}
	
	
	

	private boolean validate(BasicDBObject person) {
		//execute validation logic
        return true;
	}
}

