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
package com.xoriant.akka.mongodb.bulkimport.message;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;

public class CSVRecordBatchMsg {
	
	public CSVRecordBatchMsg(){
		records = new ArrayList<BasicDBObject>();
	}
	private int batchNo;
	private List<BasicDBObject> records;
	public int getBatchNo() {
		return batchNo;
	}
	public void setBatchNo(int batchNo) {
		this.batchNo = batchNo;
	}
	public List<BasicDBObject> getRecords() {
		return records;
	}
	public void setRecords(List<BasicDBObject> records) {
		this.records = records;
	}
	
	public void add(BasicDBObject person){
		records.add(person);
	}
	
	

}
