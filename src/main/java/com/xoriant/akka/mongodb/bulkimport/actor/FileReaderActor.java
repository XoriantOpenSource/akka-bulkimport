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

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

import com.mongodb.BasicDBObject;
import com.xoriant.akka.mongodb.bulkimport.message.BatchCompleteMsg;
import com.xoriant.akka.mongodb.bulkimport.message.CSVRecordBatchMsg;
import com.xoriant.akka.mongodb.bulkimport.message.EndOfFileMsg;

public class FileReaderActor extends UntypedActor {

	private static final String[] FILE_HEADER_MAPPING = { "Gender", "Title",
			"NameSet", "Surname", "City", "StateFull", "ZipCode" };

	// person attributes
	private static final String PERSON_TITLE = "Title";
	private static final String PERSON_GENDER = "Gender";
	private static final String PERSON_NAMESET = "NameSet";
	private static final String PERSON_SURNAME = "Surname";
	private static final String PERSON_CITY = "City";
	private static final String PERSON_STATE = "StateFull";
	private static final String PERSON_ZIPCODE = "ZipCode";

	private ActorRef mongoInsertionActor;
	
	private int batchSize;
	
	private int batchSentCounter = 0;
	
	private int batchCompleteCounter = 0;

	public FileReaderActor() {
	}

	public FileReaderActor(ActorRef mongoInsertionActor, int batchSize) {
		this.mongoInsertionActor = mongoInsertionActor;
		this.batchSize = batchSize;
	}

	@Override
	public void onReceive(Object message) throws Exception {

		if (message instanceof String) {
			readAndInsertCSV((String) message);
		}
		else if(message instanceof BatchCompleteMsg){
			batchCompleteCounter++;
			BatchCompleteMsg batchComplete = (BatchCompleteMsg)message;
			System.out.println("Reader: Batch no "+batchComplete.getBatchNo()+"complete ack ["+batchSentCounter+":"+batchCompleteCounter+"]");
			if(batchSentCounter == batchCompleteCounter){
				System.out.println("All batches completed successfully !!");
				getContext().stop(getSelf());
				
			}
		}
		else {
			unhandled(message);
		}
	}

	private void readAndInsertCSV(String filePath) {
		FileReader fileReader = null;

		CSVParser csvFileParser = null;

		// Create the CSVFormat object with the header mapping
		CSVFormat csvFileFormat = CSVFormat.EXCEL
				.withHeader(FILE_HEADER_MAPPING);

		try {

			fileReader = new FileReader(filePath);

			csvFileParser = new CSVParser(fileReader, csvFileFormat);

			List<CSVRecord> csvRecords = csvFileParser.getRecords();
			CSVRecordBatchMsg csvRecordBatch = new CSVRecordBatchMsg();
			boolean batchSent = false;
			// Skip the header row and start reading CSV records
			for (int i = 1; i < csvRecords.size(); i++) {
				CSVRecord record = csvRecords.get(i);
				BasicDBObject person = new BasicDBObject();
				person.put(PERSON_GENDER,record.get(PERSON_GENDER));
				person.put(PERSON_TITLE,record.get(PERSON_TITLE));
				person.put(PERSON_NAMESET,record.get(PERSON_NAMESET));
				person.put(PERSON_SURNAME,record.get(PERSON_SURNAME));
				person.put(PERSON_CITY,record.get(PERSON_CITY));
				person.put(PERSON_STATE,record.get(PERSON_STATE));
				person.put(PERSON_ZIPCODE,record.get(PERSON_ZIPCODE));
				csvRecordBatch.add(person);
				batchSent = false;
				if(i%batchSize == 0){
					batchSentCounter++;
					csvRecordBatch.setBatchNo(batchSentCounter);
					mongoInsertionActor.tell(csvRecordBatch, getSelf());
					csvRecordBatch = new CSVRecordBatchMsg();
					batchSent = true;
				}
				
			}
			
			// Last batch maybe pending if there are less than batch size left over records. Sending last batch of such records explicitly
			if(!batchSent){
				batchSentCounter++;
				csvRecordBatch.setBatchNo(batchSentCounter);
				mongoInsertionActor.tell(csvRecordBatch, getSelf());
			}
			mongoInsertionActor.tell(new EndOfFileMsg(), getSelf());
			System.out.println("FileReaderActor: EOF sent");

		} catch (Exception e) {
			System.out.println("Error in CsvFileReader !!!" + e.getMessage());
		} finally {
			try {
				fileReader.close();
				csvFileParser.close();
			} catch (IOException e) {
				System.out
						.println("Error while closing fileReader/csvFileParser : "
								+ e.getMessage());
			}
		}

	}

}