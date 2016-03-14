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

package org.apache.flink.test.scaling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

public class KeyGroupsITCase extends TestLogger {

	@Test
	public void testKeyGroups() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setMaxParallelism(5);
		env.setParallelism(3);

		DataStream<Tuple2<Integer, Integer>> input = env.fromElements(1, 2, 3, 4, 5, 6, 7).map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 494178923987052158L;

			@Override
			public Tuple2<Integer, Integer> map(Integer value) throws Exception {
				return Tuple2.of(value, value);
			}
		});

		DataStream<Tuple2<Integer, Integer>> result = input.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 5713085119372248196L;

			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
				return Tuple2.of(value1.f0, value1.f1 + value2.f1);
			}
		});

		result.print();

		env.execute();
	}
}
