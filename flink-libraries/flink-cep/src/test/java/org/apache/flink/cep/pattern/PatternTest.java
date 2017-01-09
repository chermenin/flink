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

import static org.apache.flink.cep.pattern.EventPattern.event;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.pattern.functions.AndFilterFunction;
import org.apache.flink.cep.pattern.functions.OrFilterFunction;
import org.apache.flink.cep.pattern.functions.SubtypeFilterFunction;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class PatternTest extends TestLogger {

	/**
	 * These test simply test that the pattern construction completes without failure
	 */

	@Test
	public void testStrictContiguity() {
		Pattern pattern = event("start").next(event("next")).next(event("end"));

		Collection<Pattern> parents;
		Collection<Pattern> parents2 = new ArrayList<>();

		parents = pattern.getParents();
		assertTrue(parents.size() == 1);
		for (Pattern parent : parents) {
			parents2.addAll(parent.getParents());
		}
		assertTrue(parents2.size() == 1);

		assertEquals(pattern.getClass(), EventPattern.class);
		assertEquals(((EventPattern) pattern).getName(), "end");

		for (Pattern parent : parents) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "next");
		}

		for (Pattern parent : parents2) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "start");
		}
	}

	@Test
	public void testNonStrictContiguity() {
		Pattern pattern =
			event("start").followedBy(event("next")).followedBy(event("end"));

		Collection<Pattern> parents;
		Collection<Pattern> parents2 = new ArrayList<>();

		parents = pattern.getParents();
		assertTrue(parents.size() == 1);
		for (Pattern parent : parents) {
			parents2.addAll(parent.getParents());
		}
		assertTrue(parents2.size() == 1);

		assertEquals(pattern.getClass(), EventPattern.class);
		assertEquals(((EventPattern) pattern).getName(), "end");

		for (Pattern parent : parents) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "next");
		}

		for (Pattern parent : parents2) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "start");
		}
	}

	@Test
	public void testStrictContiguityWithCondition() {
		Pattern pattern = EventPattern.<Event>event("start")
			.next(
				EventPattern.<Event>event("next")
					.where(new FilterFunction<Event>() {
						private static final long serialVersionUID = -7657256242101104925L;

						@Override
						public boolean filter(Event value) throws Exception {
							return value.getName().equals("foobar");
						}
					})
			)
			.next(
				EventPattern.<Event>event("end")
					.where(new FilterFunction<Event>() {
						private static final long serialVersionUID = -7597452389191504189L;

						@Override
						public boolean filter(Event value) throws Exception {
							return value.getId() == 42;
						}
					})
			);

		Collection<Pattern> parents;
		Collection<Pattern> parents2 = new ArrayList<>();

		parents = pattern.getParents();
		assertTrue(parents.size() == 1);
		for (Pattern parent : parents) {
			parents2.addAll(parent.getParents());
		}
		assertTrue(parents2.size() == 1);

		assertEquals(pattern.getClass(), EventPattern.class);
		assertEquals(((EventPattern) pattern).getName(), "end");
		assertNotNull(pattern.getFilterFunction());

		for (Pattern parent : parents) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "next");
			assertNotNull(parent.getFilterFunction());
		}

		for (Pattern parent : parents2) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "start");
			assertNull(parent.getFilterFunction());
		}
	}

	@Test
	public void testPatternWithSubtyping() {
		Pattern pattern = EventPattern.<Event>event("start")
			.next(EventPattern.<SubEvent>event("subevent"))
			.followedBy(EventPattern.<Event>event("end"));

		Collection<Pattern> parents;
		Collection<Pattern> parents2 = new ArrayList<>();

		parents = pattern.getParents();
		assertTrue(parents.size() == 1);
		for (Pattern parent : parents) {
			parents2.addAll(parent.getParents());
		}
		assertTrue(parents2.size() == 1);

		assertEquals(pattern.getClass(), EventPattern.class);
		assertEquals(((EventPattern) pattern).getName(), "end");
		assertNull(pattern.getFilterFunction());

		for (Pattern parent : parents) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "subevent");
			assertNotNull(parent.getFilterFunction());
			assertEquals(parent.getFilterFunction().getClass(), SubtypeFilterFunction.class);
		}

		for (Pattern parent : parents2) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "start");
			assertNull(parent.getFilterFunction());
		}
	}

	@Test
	public void testPatternWithSubtypingAndFilter() {
		Pattern pattern = EventPattern.<Event>event("start")
			.next(
				EventPattern.<SubEvent>event("subevent")
					.where(new FilterFunction<SubEvent>() {
						private static final long serialVersionUID = -4118591291880230304L;

						@Override
						public boolean filter(SubEvent value) throws Exception {
							return false;
						}
					})
			)
			.followedBy(EventPattern.<Event>event("end"));

		Collection<Pattern> parents;
		Collection<Pattern> parents2 = new ArrayList<>();

		parents = pattern.getParents();
		assertTrue(parents.size() == 1);
		for (Pattern parent : parents) {
			parents2.addAll(parent.getParents());
		}
		assertTrue(parents2.size() == 1);

		assertEquals(pattern.getClass(), EventPattern.class);
		assertEquals(((EventPattern) pattern).getName(), "end");
		assertNull(pattern.getFilterFunction());

		for (Pattern parent : parents) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "subevent");
			assertNotNull(parent.getFilterFunction());

			assertEquals(parent.getFilterFunction().getClass(), AndFilterFunction.class);
			assertEquals(((AndFilterFunction) parent.getFilterFunction()).getLeft().getClass(),
						 SubtypeFilterFunction.class);
			assertTrue(FilterFunction.class.isAssignableFrom(
				((AndFilterFunction) parent.getFilterFunction()).getRight().getClass()));
		}

		for (Pattern parent : parents2) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "start");
			assertNull(parent.getFilterFunction());
		}
	}

	@Test
	public void testPatternWithOrFilter() {
		Pattern pattern = EventPattern.<Event>event("start")
			.where(new FilterFunction<Event>() {
					   private static final long serialVersionUID = 3518061453394250543L;

					   @Override
					   public boolean filter(Event value) throws Exception {
						   return false;
					   }
				   }
			)
			.or(new FilterFunction<Event>() {
					private static final long serialVersionUID = 947463545810023841L;

					@Override
					public boolean filter(Event value) throws Exception {
						return false;
					}
				}
			)
			.next(
				EventPattern.<Event>event("or")
					.or(new FilterFunction<Event>() {
						private static final long serialVersionUID = -2775487887505922250L;

						@Override
						public boolean filter(Event value) throws Exception {
							return false;
						}
					})
			)
			.followedBy(EventPattern.<Event>event("end"));

		Collection<Pattern> parents;
		Collection<Pattern> parents2 = new ArrayList<>();

		parents = pattern.getParents();
		assertTrue(parents.size() == 1);
		for (Pattern parent : parents) {
			parents2.addAll(parent.getParents());
		}
		assertTrue(parents2.size() == 1);

		assertEquals(pattern.getClass(), EventPattern.class);
		assertEquals(((EventPattern) pattern).getName(), "end");
		assertNull(pattern.getFilterFunction());

		for (Pattern parent : parents) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "or");
			assertNotNull(parent.getFilterFunction());
			assertNotEquals(parent.getFilterFunction().getClass(), OrFilterFunction.class);
		}

		for (Pattern parent : parents2) {
			assertEquals(parent.getClass(), EventPattern.class);
			assertEquals(((EventPattern) parent).getName(), "start");
			assertNotNull(parent.getFilterFunction());
			assertEquals(parent.getFilterFunction().getClass(), OrFilterFunction.class);
		}
	}
}
