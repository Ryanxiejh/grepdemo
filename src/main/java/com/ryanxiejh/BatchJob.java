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

package com.ryanxiejh;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextValueInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Collection;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final ParameterTool params = ParameterTool.fromArgs(args);

		env.getConfig().setGlobalJobParameters(params);

		DataSet<String> text;

		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			// get default test text data
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			String[] g_text=new String[]{"a","b","c","a","a"};
			text = env.fromElements(g_text);
		}

		String target = params.get("filter");
		System.out.println(text.flatMap(new Tokenizer()).filter(new NaturalStringFilter(target)).count());

	}

	public static final class NaturalStringFilter implements FilterFunction<String> {
		private final String target;

		public NaturalStringFilter(String string){
			this.target = string;
		}

		@Override
		public boolean filter(String string) {
			return string.equals(target);
		}
	}

	public static final class Tokenizer implements FlatMapFunction<String, String> {

		@Override
		public void flatMap(String value, Collector<String> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(token);
				}
			}
		}
	}
}

