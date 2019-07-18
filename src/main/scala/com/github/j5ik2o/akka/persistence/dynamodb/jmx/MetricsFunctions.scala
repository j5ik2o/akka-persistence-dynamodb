package com.github.j5ik2o.akka.persistence.dynamodb.jmx

case class MetricsFunctions(
    setPutJournalRowsDuration: Long => Unit = { _ =>
    },
    addPutJournalRowsCounter: Long => Unit = { _ =>
    },
    setPutJournalRowsTotalDuration: Long => Unit = { _ =>
    },
    incrementPutJournalRowsTotalCounter: () => Unit = { () =>
    },
    setDeleteJournalRowsDuration: Long => Unit = { _ =>
    },
    addDeleteJournalRowsCounter: Long => Unit = { _ =>
    },
    setDeleteJournalRowsTotalDuration: Long => Unit = { _ =>
    },
    incrementDeleteJournalRowsTotalCounter: () => Unit = { () =>
    },
    setHighestSequenceNrTotalDuration: Long => Unit = { _ =>
    },
    incrementHighestSequenceNrTotalCounter: () => Unit = { () =>
    },
    setMessagesDuration: Long => Unit = { _ =>
    },
    incrementMessagesCounter: () => Unit = { () =>
    },
    setMessagesTotalDuration: Long => Unit = { _ =>
    },
    incrementMessagesTotalCounter: () => Unit = { () =>
    },
    setAllPersistenceIdsDuration: Long => Unit = { _ =>
    },
    addAllPersistenceIdsCounter: Long => Unit = { _ =>
    },
    setAllPersistenceIdsTotalDuration: Long => Unit = { _ =>
    },
    incrementAllPersistenceIdsTotalCounter: () => Unit = { () =>
    },
    setEventsByTagDuration: Long => Unit = { _ =>
    },
    addEventsByTagCounter: Long => Unit = { _ =>
    },
    setEventsByTagTotalDuration: Long => Unit = { _ =>
    },
    incrementEventsByTagTotalCounter: () => Unit = { () =>
    },
    setJournalSequenceDuration: Long => Unit = { _ =>
    },
    addJournalSequenceCounter: Long => Unit = { _ =>
    },
    setJournalSequenceTotalDuration: Long => Unit = { _ =>
    },
    incrementJournalSequenceTotalCounter: () => Unit = { () =>
    }
)
