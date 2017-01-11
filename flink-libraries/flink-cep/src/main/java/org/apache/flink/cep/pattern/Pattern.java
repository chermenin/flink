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

package org.apache.flink.cep.pattern;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

/**
 * Base class for a pattern definition.
 * <p>
 * A pattern definition is used by {@link org.apache.flink.cep.nfa.compiler.NFACompiler} to create
 * a {@link NFA}.
 *
 * <pre>{@code
 * Pattern<T, F> pattern = Pattern.<T>begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MyFilterFunction());
 * }
 * </pre>
 */
public class Pattern {

	// previous pattern operator
	Collection<Pattern> parents;

	// window length in which the pattern match has to occur
	Time windowTime;

	protected Pattern() {
		this.parents = new HashSet<>();
	}

	private Pattern(final Pattern... parents) {
		this.parents = new HashSet<>(Arrays.asList(parents));
	}

	public static Pattern or(final Pattern... patterns) {
		return new Pattern(patterns);
	}

	public Collection<Pattern> getParents() {
		return parents;
	}

	protected void setSkipped() {
		for (Pattern parent : parents) {
			parent.setSkipped();
		}
	}

	public Time getWindowTime() {
		long time = this.windowTime != null ? this.windowTime.toMilliseconds() : -1L;
		for (Pattern parent : parents) {
			if (parent.getWindowTime() != null && (
				parent.getWindowTime().toMilliseconds() < time || time < 0
			)) {
				// the window time is the global minimum of all window times of each state
				time = parent.getWindowTime().toMilliseconds();
			}
		}
		return time < 0 ? null : Time.milliseconds(time);
	}

	/**
	 * Defines the maximum time interval for a matching pattern. This means that the time gap
	 * between first and the last event must not be longer than the window time.
	 *
	 * @param windowTime Time of the matching window
	 * @return The same pattenr operator with the new window length
	 */
	public Pattern within(Time windowTime) {
		if (windowTime != null) {
			this.windowTime = windowTime;
		}
		return this;
	}

	/**
	 * Appends a new pattern operator to the existing one. The new pattern operator enforces strict
	 * temporal contiguity. This means that the whole pattern only matches if an event which matches
	 * this operator directly follows the preceding matching event. Thus, there cannot be any
	 * events in between two matching events.
	 *
	 * @param pattern New pattern operator
	 * @return A new pattern operator which is appended to this pattern operator
	 */
	public Pattern next(final Pattern pattern) {
		if (pattern instanceof EventPattern) {
			pattern.parents = Collections.singleton(this);
		} else {
			for (Pattern parent : pattern.parents) {
				parent.parents = Collections.singleton(this);
			}
		}
		return pattern;
	}

	/**
	 * Appends a new pattern operator to the existing one. The new pattern operator enforces
	 * non-strict temporal contiguity. This means that a matching event of this operator and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param pattern New pattern operator
	 * @return A new pattern operator which is appended to this pattern operator
	 */
	public Pattern followedBy(final Pattern pattern) {
		this.setSkipped();
		next(pattern);
		return pattern;
	}

	public FilterFunction getFilterFunction() {
		return null;
	}

	@Internal
	public <T> Collection<? extends Tuple2<State<T>, Pattern>> setStates(Map<String, State<T>> states, State<T> succeedingState, FilterFunction<T> filterFunction) {
		Collection<Tuple2<State<T>, Pattern>> startStates = new ArrayList<>();
		for (Pattern parent : parents) {
			startStates.addAll(parent.setStates(states, succeedingState, filterFunction));
		}
		return startStates;
	}

	@Internal
	public Pattern optimize(Class<?> clazz) {
		ArrayList<Pattern> optimizedParents = new ArrayList<>();
		for (Pattern parent : parents) {
			optimizedParents.add(parent.optimize(clazz));
		}
		Pattern optimizedPattern =
			new Pattern(optimizedParents.toArray(new Pattern[optimizedParents.size()]));
		optimizedPattern.windowTime = windowTime;
		return optimizedPattern;
	}
}
