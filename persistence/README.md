# Persistence module

This module defines interface used by the scheduler to store its internal state.

Users of the USI can implement `PodRecordRepository` and `ReservationRecordRepository`
classes to support different data stores.

