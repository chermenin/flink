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

package org.apache.flink.cep.scala.pattern

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.cep
import org.apache.flink.cep.pattern.{EventPattern => JEventPattern}

/**
  * Base class for a pattern definition.
  *
  * A pattern definition is used by [[org.apache.flink.cep.nfa.compiler.NFACompiler]] to create
  * a [[org.apache.flink.cep.nfa.NFA]].
  *
  * {{{
  * Pattern<T, F> pattern = Pattern.<T>begin("start")
  * .next("middle").subtype(F.class)
  * .followedBy("end").where(new MyFilterFunction());
  * }
  * }}}
  *
  * @param jEventPattern Underlying Java API Pattern
  * @tparam T Base type of the elements appearing in the pattern
  */
class EventPattern[T](jEventPattern: JEventPattern[T])
  extends Pattern(jEventPattern) {

  /**
    *
    * @return Name of the pattern operator
    */
  def getName: String = jEventPattern.getName

  /**
    *
    * @return Filter condition for an event to be matched
    */
  override def getFilterFunction: Option[FilterFunction[T]] =
    Option(jEventPattern.getFilterFunction)
//
//  /**
//    * Applies a subtype constraint on the current pattern operator. This means that an event has
//    * to be of the given subtype in order to be matched.
//    *
//    * @param clazz Class of the subtype
//    * @tparam S Type of the subtype
//    * @return The same pattern operator with the new subtype constraint
//    */
//  def subtype[S <: F](clazz: Class[S]): EventPattern[T, S] = {
//    EventPattern[T, S](jEventPattern.subtype(clazz))
//  }

  /**
    * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
    *
    * @param filter Filter condition
    * @return The same pattern operator where the new filter condition is set
    */
  def where(filter: FilterFunction[T]): EventPattern[T] = {
    EventPattern[T](jEventPattern.where(filter))
  }

  /**
    * Specifies a filter condition which is ORed with an existing filter function.
    *
    * @param filter Or filter function
    * @return The same pattern operator where the new filter condition is set
    */
  def or(filter: FilterFunction[T]): EventPattern[T] = {
    EventPattern[T](jEventPattern.or(filter))
  }

  /**
    * Specifies a filter condition which is ANDed with an existing filter function.
    *
    * @param filter Or filter function
    * @return The same pattern operator where the new filter condition is set
    */
  def and(filter: FilterFunction[T]): EventPattern[T] = {
    EventPattern[T](jEventPattern.and(filter))
  }

  /**
    * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
    *
    * @param filterFun Filter condition
    * @return The same pattern operator where the new filter condition is set
    */
  def where(filterFun: T => Boolean): EventPattern[T] = {
    val filter = new FilterFunction[T] {
      val cleanFilter: (T) => Boolean = cep.scala.cleanClosure(filterFun)

      override def filter(value: T): Boolean = cleanFilter(value)
    }
    where(filter)
  }

  /**
    * Specifies a filter condition which is ORed with an existing filter function.
    *
    * @param filterFun Filter condition
    * @return The same pattern operator where the new filter condition is set
    */
  def or(filterFun: T => Boolean): EventPattern[T] = {
    val filter = new FilterFunction[T] {
      val cleanFilter: (T) => Boolean = cep.scala.cleanClosure(filterFun)

      override def filter(value: T): Boolean = cleanFilter(value)
    }
    or(filter)
  }

  /**
    * Specifies a filter condition which is ORed with an existing filter function.
    *
    * @param filterFun Filter condition
    * @return The same pattern operator where the new filter condition is set
    */
  def and(filterFun: T => Boolean): EventPattern[T] = {
    val filter = new FilterFunction[T] {
      val cleanFilter: (T) => Boolean = cep.scala.cleanClosure(filterFun)

      override def filter(value: T): Boolean = cleanFilter(value)
    }
    and(filter)
  }
}

object EventPattern {

  /**
    * Constructs a new Pattern by wrapping a given Java API Pattern
    *
    * @param name Pattern name.
    * @tparam T Base type of the elements appearing in the pattern
    * @return New wrapping Pattern object
    */
  def apply[T](name: String) =
    new EventPattern[T](JEventPattern.event[T](name))

  def apply[T](name: String, clazz: Class[T]) =
    new EventPattern[T](JEventPattern.subevent[T](name, clazz))

  def apply[T](jEventPattern: JEventPattern[T]): EventPattern[T] =
    new EventPattern[T](jEventPattern)
}
