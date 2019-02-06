package com.mesosphere.utils.zookeeper

import org.apache.curator.{RetryPolicy, RetrySleeper}

object NoRetryPolicy extends RetryPolicy {
  override def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper): Boolean = false
}
