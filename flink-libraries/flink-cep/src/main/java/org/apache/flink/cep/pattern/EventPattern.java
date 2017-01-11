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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.functions.AndFilterFunction;
import org.apache.flink.cep.pattern.functions.BiFilterFunction;
import org.apache.flink.cep.pattern.functions.OrFilterFunction;
import org.apache.flink.cep.pattern.functions.SubtypeFilterFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class EventPattern<T> extends Pattern {

	// name of the pattern operator
	private final String name;

	// pattern can be skipped (follow by pattern)
	private boolean canSkip;

	// filter condition for an event to be matched
	private FilterFunction<T> filterFunction;

	private EventPattern(String name) {
		this.name = name;
	}

	public static <T> EventPattern<T> event(final String name) {
		return new EventPattern<>(name);
	}

	public static <T> EventPattern<T> subevent(final String name, final Class<T> clazz) {
		return new EventPattern<T>(name).
			where(new SubtypeFilterFunction<>(clazz), false);
	}

	public String getName() {
		return name;
	}

	@Override
	public FilterFunction<T> getFilterFunction() {
		return filterFunction;
	}

	@Override
	protected void setSkipped() {
		canSkip = true;
	}

	/**
	 * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
	 *
	 * @param filterFunction Filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public EventPattern<T> where(FilterFunction<T> filterFunction) {
		return where(filterFunction, false);
	}

	/**
	 * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
	 *
	 * @param filterFunction Filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public EventPattern<T> and(FilterFunction<T> filterFunction) {
		return where(filterFunction, false);
	}

	/**
	 * Specifies a filter condition which is ORed with an existing filter function.
	 *
	 * @param filterFunction OR filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public EventPattern<T> or(FilterFunction<T> filterFunction) {
		return where(filterFunction, true);
	}

	private EventPattern<T> where(FilterFunction<T> filterFunction, boolean orFunction) {
		ClosureCleaner.clean(filterFunction, true);

		if (this.filterFunction == null) {
			this.filterFunction = filterFunction;
		} else if (orFunction) {
			this.filterFunction = new OrFilterFunction<>(this.filterFunction, filterFunction);
		} else {
			this.filterFunction = new AndFilterFunction<>(this.filterFunction, filterFunction);
		}

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <F> Collection<? extends Tuple2<State<F>, Pattern>>
	setStates(Map<String, State<F>> states, State<F> succeedingState, FilterFunction<F> filterFunction) {

		Collection<Tuple2<State<F>, Pattern>> startStates = new ArrayList<>();

		// get current state
		State<F> currentState = succeedingState;
		if (name != null) {
			if (states.containsKey(name)) {
				currentState = states.get(name);
			} else {
				currentState = new State<>(name, State.StateType.Normal);
				states.put(currentState.getName(), currentState);
			}
		}

		startStates.addAll(
			super.setStates(states, currentState, (FilterFunction<F>) this.filterFunction)
		);

		// add transitions for current state
		if (name != null && !currentState.isFinal()) {

			currentState.addStateTransition(new StateTransition<>(
				StateTransitionAction.TAKE,
				succeedingState,
				filterFunction));
		}

		if (canSkip) {
			currentState.addStateTransition(new StateTransition<>(
				StateTransitionAction.IGNORE,
				currentState, null));
		}

		if (getParents().isEmpty()) {
			startStates.add(Tuple2.<State<F>, Pattern>of(currentState, this));
		}

		return startStates;
	}

	@Override
	public Pattern optimize(Class<?> clazz) {
		filterFunction = optimizeFunction(filterFunction, clazz);
		return super.optimize(clazz);
	}

	@SuppressWarnings("unchecked")
	private FilterFunction<T> optimizeFunction(FilterFunction<T> function, Class<?> clazz) {
		if (function instanceof SubtypeFilterFunction &&
			((SubtypeFilterFunction) function).getSubtype().isAssignableFrom(clazz)) {
			return null;
		} else if (function instanceof BiFilterFunction) {
			FilterFunction<T> left =
				optimizeFunction(((BiFilterFunction) function).getLeft(), clazz);

			FilterFunction<T> right =
				optimizeFunction(((BiFilterFunction) function).getRight(), clazz);

			if (left == null) {
				return right;
			} else if (right == null) {
				return left;
			} else {
				return
					function instanceof AndFilterFunction
					? new AndFilterFunction<>(left, right)
					: new OrFilterFunction<>(left, right);
			}
		} else {
			return function;
		}
	}
}
