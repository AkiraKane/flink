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

package org.apache.flink.runtime.jobgraph;

public enum ScheduleMode {

	/**
	 * Schedule tasks from all sources to sinks with lazy deployment of receiving tasks.
	 */
	FROM_SOURCES,

	/**
	 * Schedule tasks from manually configured sources.
	 */
	BATCH_FROM_SOURCES,

	BACKTRACKING,

	/**
	 * Schedule tasks all at once instead of lazy deployment of receiving tasks.
	 */
	ALL

}
