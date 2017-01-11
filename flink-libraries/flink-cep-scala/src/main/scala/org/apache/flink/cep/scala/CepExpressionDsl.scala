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
import org.apache.flink.streaming.api.windowing.time.Time

import scala.language.implicitConversions

trait CepImplicitPatternOperations {
  private[flink] def pattern: Pattern
  def ||(other: Pattern): Pattern = Pattern.or(pattern, other)
  def ->(other: Pattern): Pattern = pattern.next(other)
  def ->>(other: Pattern): Pattern = pattern.followedBy(other)
  def in(windowTime: Time): Pattern = pattern.within(windowTime)
}

trait CepImplicitEventPatternOperations[T]
  extends CepImplicitPatternOperations {
  private[flink] def pattern: EventPattern[T]
}

trait CepImplicitExpressionConversions {
  implicit class ResolvedPattern(p: Pattern)
    extends CepImplicitPatternOperations {
    def pattern: Pattern = p
  }

  implicit class ResolvedEventPattern[T](p: EventPattern[T])
    extends CepImplicitEventPatternOperations[T] {
    def pattern: EventPattern[T] = p
  }
}

// scalastyle:off
object & {
// scalastyle:on

  def apply[T: Manifest](name: String): EventPattern[T] =
    if (manifest[T].runtimeClass == classOf[Nothing]) {
      new EventPattern(JEventPattern.event(name))
    } else {
      new EventPattern(JEventPattern.subevent(
        name, manifest[T].runtimeClass.asInstanceOf[Class[T]]))
    }

  def apply[T: Manifest](name: String, filterFun: T => Boolean): EventPattern[T] =
    new EventPattern[T](JEventPattern.subevent(
      name, manifest[T].runtimeClass.asInstanceOf[Class[T]])).where(filterFun)
}
