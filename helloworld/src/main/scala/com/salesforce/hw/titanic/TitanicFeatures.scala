/*
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.hw.titanic

import java.io.Serializable

import com.salesforce.hw.titanic.TitanicFeatures._
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._

trait TitanicFeatures extends Serializable {
  val survived = FeatureBuilder.RealNN[Passenger2].extract(new Survived).asResponse
  val pClass = FeatureBuilder.PickList[Passenger2].extract(new PClass).asPredictor
  val name = FeatureBuilder.Text[Passenger2].extract(new Name).asPredictor
  val sex = FeatureBuilder.PickList[Passenger2].extract(new Sex).asPredictor
  val age = FeatureBuilder.Real[Passenger2].extract(new Age).asPredictor
  val sibSp = FeatureBuilder.PickList[Passenger2].extract(new SibSp).asPredictor
  val parch = FeatureBuilder.PickList[Passenger2].extract(new Parch).asPredictor
  val ticket = FeatureBuilder.PickList[Passenger2].extract(new Ticket).asPredictor
  val fare = FeatureBuilder.Real[Passenger2].extract(new Fare).asPredictor
  val cabin = FeatureBuilder.PickList[Passenger2].extract(new Cabin).asPredictor
  val embarked = FeatureBuilder.PickList[Passenger2].extract(new Embarked).asPredictor
}

object TitanicFeatures {
  abstract class TitanicFeatureFunc[T] extends Function[Passenger2, T] with Serializable

  class RealExtract[T <: Real](f: Passenger2 => Option[Double], f1: Option[Double] => T) extends TitanicFeatureFunc[T] {
    override def apply(v1: Passenger2): T = f1(f(v1))
  }

  class PickListExtract(f: Passenger2 => Option[_]) extends TitanicFeatureFunc[PickList] {
    override def apply(v1: Passenger2): PickList = f(v1).map(_.toString).toPickList
  }

  class Survived extends RealExtract(p => Option(p.Survived).map(_.toDouble), _.get.toRealNN)

  class PClass extends PickListExtract(p => Option(p.Pclass))

  class Sex extends PickListExtract(p => Option(p.Sex))

  class SibSp extends PickListExtract(p => Option(p.SibSp))

  class Parch extends PickListExtract(p => Option(p.Parch))

  class Ticket extends PickListExtract(p => Option(p.Ticket))

  class Embarked extends PickListExtract(p => Option(p.Embarked))

  class Cabin extends PickListExtract(p => Option(p.Cabin))

  class Name extends TitanicFeatureFunc[Text] with Serializable {
    override def apply(v1: Passenger2): Text = Option(v1.Name).toText
  }

  class Age extends RealExtract(p => p.Age, _.toReal)

  class Fare extends RealExtract(p => Option(p.Fare), _.toReal)
}
