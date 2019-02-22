package com.mesosphere.usi.storage

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.{Answer, OngoingStubbing}
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

  def noMoreInteractions(mocks: AnyRef*): Unit = {
    M.verifyNoMoreInteractions(mocks: _*)
  }

  def reset(mocks: AnyRef*): Unit = {
    M.reset(mocks: _*)
  }

  class MockAnswer[T](function: Array[AnyRef] => T) extends Answer[T] {
    def answer(invocation: InvocationOnMock): T = {
      function(invocation.getArguments)
    }
  }

  implicit class Stubbed[T](c: => T) {
    def returns(t: T, t2: T*): OngoingStubbing[T] = {
      if (t2.isEmpty) M.when(c).thenReturn(t)
      else
        t2.foldLeft(M.when(c).thenReturn(t)) { (res, cur) =>
          res.thenReturn(cur)
        }
    }
    def answers(function: Array[AnyRef] => T) = M.when(c).thenAnswer(new MockAnswer(function))
    def throws[E <: Throwable](e: E*): OngoingStubbing[T] = {
      if (e.isEmpty) throw new java.lang.IllegalArgumentException("The parameter passed to throws must not be empty")
      e.drop(1).foldLeft(M.when(c).thenThrow(e.head)) { (res, cur) =>
        res.thenThrow(cur)
      }
    }
  }
}
