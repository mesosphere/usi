package com.mesosphere.utils

import org.mockito.verification.VerificationMode
import org.mockito.{Mockito => M}
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.mockito.MockitoSugar

/**
  * ScalaTest mockito support is quite limited and ugly.
  */
trait Mockito extends MockitoSugar with PatienceConfiguration {

  def equalTo[T](t: T) = org.mockito.Matchers.eq(t)
  def eq[T](t: T) = org.mockito.Matchers.eq(t)
  def any[T] = org.mockito.Matchers.any[T]
  def anyBoolean = org.mockito.Matchers.anyBoolean
  def anyString = org.mockito.Matchers.anyString
  def same[T](value: T) = org.mockito.Matchers.same(value)
  def verify[T](t: T, mode: VerificationMode = times(1)) = M.verify(t, mode)
  def times(num: Int) = M.times(num)
  def timeout(millis: Int) = M.timeout(millis.toLong)
  def withinTimeout() = M.timeout(patienceConfig.timeout.millisPart)
  def atLeastOnce = M.atLeastOnce()
  def once = M.times(1)
  def atLeast(num: Int) = M.atLeast(num)
  def atMost(num: Int) = M.atMost(num)
  def never = M.never()

  def inOrder(mocks: AnyRef*) = M.inOrder(mocks: _*)
  def noMoreInteractions(mocks: AnyRef*): Unit = M.verifyNoMoreInteractions(mocks: _*)
  def reset(mocks: AnyRef*): Unit = M.reset(mocks: _*)
}
