// @source workers/slopbot/value-requests.ts
// @source workers/slopbot/index.ts
// @source workers/slopbot/value-consumption.ts
// @source workers/slopbot/value-consumer.ts
// @source workers/slopbot/value-work-unit-consumption.ts
// @evidence workers/slopbot/value-branch-requeue.test.ts
module bot_value_branch_requeue {
  var s: { branch: int, recordBranch: int, revision: int, changedRevision: int,
    requestedRevision: int, scannedRevision: int, pending: bool,
    inWindow: bool, active: bool, published: int, readFailed: bool }
  action init = s' = { branch: 0, recordBranch: 0, revision: 1,
    changedRevision: 0, requestedRevision: 0, scannedRevision: 0,
    pending: false, inWindow: true, active: true, published: 1,
    readFailed: false }
  def current(state) = state.recordBranch == state.branch
  action changeBranch(target) = all {
    s.revision < 4, target != s.branch,
    s' = s.with("branch", target).with("revision", s.revision + 1)
      .with("changedRevision", s.revision + 1),
  }
  action otherRevision = all { s.revision < 4,
    s' = s.with("revision", s.revision + 1) }
  action setActive(active) = all { s.revision < 4, active != s.active,
    s' = s.with("active", active).with("revision", s.revision + 1) }
  action leaveWindow = s' = s.with("inWindow", false)
  action scheduledRun = {
    val queues = s.changedRevision > s.requestedRevision and s.inWindow
      and s.active
    s' = s.with("scannedRevision", s.revision)
      .with("requestedRevision",
        if (queues) s.changedRevision else s.requestedRevision)
      .with("pending", s.pending or queues)
  }
  action consume = all { s.pending, s.active,
    s' = s.with("pending", false)
      .with("recordBranch", s.branch)
      .with("published", s.published + if (current(s)) 0 else 1),
  }
  def validBranch(branch) = branch >= 0
  action readHistory = s' = s.with("readFailed", not(validBranch(s.recordBranch)))
  action step = {
    nondet target = Set(0, 1, 2).oneOf()
    nondet active = Set(false, true).oneOf()
    any { changeBranch(target), otherRevision, setActive(active), leaveWindow,
      scheduledRun, consume, readHistory }
  }
  val requestedOnlyAfterChange = s.requestedRevision <= s.changedRevision
  val staleIsQueuedAfterRun = (not(current(s)) and s.inWindow and s.active
    and s.scannedRevision == s.revision) implies s.pending
  val historyReadable = not(s.readFailed)
  val invariant = requestedOnlyAfterChange and staleIsQueuedAfterRun
    and historyReadable
  run branchChangeQueuesAndConsumerRestoresCurrentTest = init
    .then(changeBranch(1)).expect(not(current(s)) and not(s.pending))
    .then(scheduledRun).expect(s.pending and s.requestedRevision == 2)
    .then(consume).expect(current(s) and s.published == 2 and invariant)
  run repeatedRunDoesNotQueueTwiceTest = init.then(changeBranch(1))
    .then(scheduledRun).then(consume).then(scheduledRun)
    .expect(not(s.pending) and current(s) and s.requestedRevision == 2)
  run unchangedBranchQueuesNothingTest = init.then(otherRevision)
    .then(scheduledRun).expect(not(s.pending) and s.requestedRevision == 0)
  run pausedProjectWaitsUntilActiveTest = init.then(changeBranch(1))
    .then(setActive(false)).then(scheduledRun).expect(not(s.pending))
    .then(setActive(true)).then(scheduledRun)
    .expect(s.pending and s.requestedRevision == 2 and invariant)
  run outsideWindowIsNotQueuedTest = init.then(changeBranch(1))
    .then(leaveWindow).then(scheduledRun).expect(not(s.pending) and invariant)
  run branchRestoredRecordStaysCurrentTest = init.then(changeBranch(1))
    .then(changeBranch(0)).then(scheduledRun).then(consume)
    .expect(current(s) and s.published == 1)
  run formerBranchRecordReadsAsStaleTest = init.then(changeBranch(1))
    .then(readHistory).expect(not(s.readFailed) and not(current(s)))
    .then(scheduledRun).then(consume).then(readHistory)
    .expect(current(s) and invariant)
}
