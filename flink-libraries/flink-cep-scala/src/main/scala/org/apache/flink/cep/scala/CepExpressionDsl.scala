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

package org.apache.flink.cep.scala

import org.apache.flink.cep.scala.pattern.{EventPattern, Pattern}
import org.apache.flink.cep.pattern.{EventPattern => JEventPattern}

import scala.language.implicitConversions

trait CepImplicitPatternOperations[T, F <: T] {
  private[flink] def pattern: Pattern[T, F]
  def ||(other: Pattern[T, _ <: T]): Pattern[T, T] = Pattern.or(pattern, other)
  def ->(other: Pattern[T, _ <: T]): Pattern[T, T] = pattern.next(other)
  def ->>(other: Pattern[T, _ <: T]): Pattern[T, T] = pattern.followedBy(other)
}

trait CepImplicitEventPatternOperations[T, F <: T]
  extends CepImplicitPatternOperations[T, F] {
  private[flink] def pattern: EventPattern[T, F]
}

trait CepImplicitExpressionConversions {
  implicit class ResolvedPattern[T, F <: T](p: Pattern[T, F])
    extends CepImplicitPatternOperations[T, F] {
    def pattern: Pattern[T, F] = p
  }

  implicit class ResolvedEventPattern[T, F <: T](p: EventPattern[T, F])
    extends CepImplicitEventPatternOperations[T, F] {
    def pattern: EventPattern[T, F] = p
  }
}

// scalastyle:off
object & {

  def apply[T](name: String): EventPattern[T, T] =
    new EventPattern[T, T](JEventPattern.event(name))

  def apply[T](name: String, clazz: Class[_ <: T]): EventPattern[T, _ <: T] =
    new EventPattern[T, T](JEventPattern.event(name)).subtype(clazz)
}
// scalastyle:on
