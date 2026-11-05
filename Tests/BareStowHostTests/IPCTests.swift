import BareStowHost
import Foundation
import XCTest

final class IPCTests: XCTestCase {
  func testResolvesReadyWhenTheWorkerSignals() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    try await worker.send("exit", ["code": 0])
    worker.close()
    await ipc.wait()
  }

  func testSendsTheGraceToTheWorker() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 2.5)
    let worker = Worker(right)

    let message = await worker.receive("configure")

    XCTAssertEqual(message?["grace"] as? Int, 2500)

    try await worker.send("exit", ["code": 0])
    worker.close()
    await ipc.wait()
  }

  func testFailsReadyWhenTheWorkerErrorsBeforeReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.sendError("boom")

    await assertReadyFails(ipc, code: "WORKER_ERROR")

    _ = await worker.receive("terminate")
    worker.close()
  }

  func testFailsReadyWhenTheWorkerExitsBeforeReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("exit", ["code": 1])

    await assertReadyFails(ipc, code: "WORKER_EXITED", exitCode: 1)

    _ = await worker.receive("terminate")
    worker.close()
  }

  func testFailsReadyWhenTheTransportClosesWhileTerminatingBeforeReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    let closing = Task { await ipc.close() }

    _ = await worker.receive("terminate")
    worker.close()

    await assertReadyFails(ipc, code: "WORKER_CLOSED")
    await closing.value
  }

  func testIgnoresExitAfterReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    try await worker.send("exit", ["code": 1])
    worker.close()
    await ipc.wait()

    try await ipc.ready()
  }

  func testReportsTheExitCode() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("ready")
    try await worker.send("exit", ["code": 7])
    worker.close()

    let code = await ipc.wait()

    XCTAssertEqual(code, 7)
    XCTAssertEqual(ipc.exitCode, 7)
  }

  func testCarriesUserData() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.write(Data("payload".utf8))

    var iterator = ipc.makeAsyncIterator()

    let received = try await iterator.next()

    try await ipc.write(Data("reply".utf8))

    let replied = await worker.data()

    try await worker.send("exit", ["code": 0])
    worker.close()

    let end = try await iterator.next()

    XCTAssertEqual(received, Data("payload".utf8))
    XCTAssertEqual(replied, Data("reply".utf8))
    XCTAssertNil(end)
  }

  func testTerminateReturnsTheExitCode() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    let exiting = Task {
      _ = await worker.receive("terminate")
      try await worker.send("exit", ["code": 0])
      worker.close()
    }

    let code = await ipc.terminate()

    try await exiting.value

    XCTAssertEqual(code, 0)
  }

  func testCloseSendsTerminateToTheWorker() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    let closing = Task {
      _ = await worker.receive("terminate")
      worker.close()
    }

    await ipc.close()
    await closing.value
  }

  func testWaitReturnsOnceTheExitIsReported() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.close()
    ipc.exited(code: 0)

    let code = await ipc.wait()

    XCTAssertNil(code)
  }

  func testFailsReadyAndTerminatesTheWorkerOnTimeout() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, timeout: 0.01)
    let worker = Worker(right)

    await assertReadyFails(ipc, code: "WORKER_TIMEOUT")

    _ = await worker.receive("terminate")
    worker.close()
  }

  func testTerminatesTheWorkerWhenReadyIsCancelled() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    let waiting = Task { try await ipc.ready() }

    waiting.cancel()

    do {
      try await waiting.value
      XCTFail("Expected ready to be cancelled")
    } catch {
      XCTAssert(error is CancellationError)
    }

    _ = await worker.receive("terminate")
    worker.close()

    await assertReadyFails(ipc, code: "WORKER_CLOSED")
  }

  func testTerminateKillsTheWorkerAfterTheGracePeriod() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 0.01)
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    let code = await ipc.terminate()

    XCTAssertNil(code)
    XCTAssertTrue(left.closed)
  }

  func testCloseKillsTheWorkerAfterTheGracePeriod() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 0.01)

    await ipc.close()

    XCTAssertTrue(left.closed)
  }

  func testTerminateReturnsOnceTheWorkerHasExited() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.close()
    ipc.exited(code: nil, signal: 9)

    await assertReadyFails(ipc, code: "WORKER_KILLED")

    let code = await ipc.terminate()

    XCTAssertNil(code)
  }

  func testExitedBeforeReadyFailsReadyWithTheExitCode() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 0.01)

    ipc.exited(code: 3)

    do {
      try await ipc.ready()
      XCTFail("ready succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_EXITED")
      XCTAssertEqual(error.exitCode, 3)
    }

    await ipc.close()
  }

  func testExitedAfterReadyFailsWithTheSignal() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 0.01)
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    ipc.exited(code: 137, signal: 9)

    do {
      for try await _ in ipc {}
      XCTFail("iteration succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_KILLED")
      XCTAssertEqual(error.exitCode, 137)
      XCTAssertEqual(error.signal, 9)
    }

    await ipc.close()
  }

  func testExitedIsIgnoredAfterAnExitFrame() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    try await worker.send("exit", ["code": 0])
    worker.close()
    await ipc.wait()

    ipc.exited(code: 0)

    for try await _ in ipc {}
  }

  func testExitedIsIgnoredWhileTerminating() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 0.01)
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    let terminated = Task { await ipc.terminate() }

    _ = await worker.receive("terminate")

    ipc.exited(code: nil, signal: 9)

    let code = await terminated.value

    XCTAssertNil(code)

    for try await _ in ipc {}
  }

  func testHoldsTheEndOfAWorkerThatCrashesAfterReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    worker.close()
    await seenEnd(left)

    ipc.exited(code: nil, signal: 11)

    do {
      for try await _ in ipc {}
      XCTFail("iteration succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_KILLED")
      XCTAssertEqual(error.signal, 11)
    }

    await ipc.wait()
  }

  func testHoldsTheEndOfAWorkerThatCrashesBeforeReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })

    right.close()
    await seenEnd(left)

    ipc.exited(code: 1)

    await assertReadyFails(ipc, code: "WORKER_EXITED", exitCode: 1)

    await ipc.wait()
  }

  func testExitedAppliesAfterACloseThatStartsAfterTheEnd() async throws {
    let (left, right) = pair()
    let killed = Flag()
    let ipc = IPC(left, kill: { killed.set() }, grace: 0.01)
    let worker = Worker(right)

    try await worker.send("ready")
    try await ipc.ready()

    worker.close()
    await seenEnd(left)

    let closing = Task { await ipc.close() }

    try await Task.sleep(nanoseconds: 50_000_000)

    ipc.exited(code: 2)

    await closing.value

    do {
      for try await _ in ipc {}
      XCTFail("iteration succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_EXITED")
      XCTAssertEqual(error.exitCode, 2)
    }

    XCTAssertFalse(killed.value)
  }

  func testFailsACrashedWorkerWithItsExitRatherThanATimeout() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, timeout: 0.05)

    right.close()
    try await Task.sleep(nanoseconds: 100_000_000)

    ipc.exited(code: 1)

    await assertReadyFails(ipc, code: "WORKER_EXITED", exitCode: 1)
  }

  func testFailsACrashedWorkerWithItsExitRatherThanATransportFailure() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.fail(Broken())
    await seenEnd(left)

    ipc.exited(code: nil, signal: 11)

    do {
      try await ipc.ready()
      XCTFail("ready succeeded")
    } catch let error as WorkerError {
      let cause = error.underlyingError as? WorkerError
      XCTAssertEqual(error.code, "WORKER_KILLED")
      XCTAssertEqual(cause?.code, "TRANSPORT_ERROR")
      XCTAssertTrue(cause?.underlyingError is Broken)
    }
  }

  func testKillsAWorkerWhoseTransportFailsAndThatDoesNotExit() async throws {
    let (left, _) = pair()
    let killed = Flag()
    let ipc = IPC(left, kill: { killed.set() }, grace: 0.01)

    left.fail(Broken())

    do {
      try await ipc.ready()
      XCTFail("ready succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "TRANSPORT_ERROR")
      XCTAssertTrue(error.underlyingError is Broken)
    }

    XCTAssertTrue(killed.value)
  }

  func testTreatsAResetTransportAsClosed() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.fail(POSIXError(.ECONNRESET))
    await seenEnd(left)

    ipc.exited(code: 1)

    do {
      try await ipc.ready()
      XCTFail("ready succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_EXITED")
      XCTAssertNil(error.underlyingError)
    }
  }

  func testFailsWithATransportErrorWhenWritesFail() async throws {
    let (left, _) = pair(failingWritesWith: Broken())
    let killed = Flag()
    let ipc = IPC(
      left,
      kill: {
        killed.set()
        left.close()
      }, grace: 0.01)

    do {
      try await ipc.ready()
      XCTFail("ready succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "TRANSPORT_ERROR")
      XCTAssertTrue(error.underlyingError is Broken)
    }

    XCTAssertTrue(killed.value)

    do {
      try await ipc.write(Data("late".utf8))
      XCTFail("write succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_CLOSED")
    }
  }

  func testWaitsForTheExitOfAWorkerWhoseWritesFail() async throws {
    let (left, _) = pair(failingWritesWith: Broken())
    let ipc = IPC(left, kill: { left.close() })

    do {
      try await ipc.write(Data("payload".utf8))
      XCTFail("write succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "TRANSPORT_ERROR")
      XCTAssertTrue(error.underlyingError is Broken)
    }

    ipc.exited(code: 3)

    do {
      try await ipc.ready()
      XCTFail("ready succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.code, "WORKER_EXITED")
      XCTAssertEqual(error.exitCode, 3)
      XCTAssertEqual((error.underlyingError as? WorkerError)?.code, "TRANSPORT_ERROR")
    }
  }

  private func seenEnd(_ end: End) async {
    await end.ended()
    try? await Task.sleep(nanoseconds: 10_000_000)
  }

  private func assertReadyFails(
    _ ipc: IPC,
    code: String,
    exitCode: Int32? = nil,
    file: StaticString = #filePath,
    line: UInt = #line
  ) async {
    do {
      try await ipc.ready()
      XCTFail("ready succeeded", file: file, line: line)
    } catch {
      let error = error as? WorkerError
      XCTAssertEqual(error?.code, code, file: file, line: line)
      XCTAssertEqual(error?.exitCode, exitCode, file: file, line: line)
    }
  }
}

struct Broken: Error {}

final class Flag: @unchecked Sendable {
  private let lock = NSLock()
  private var isSet = false

  var value: Bool {
    lock.lock()
    defer { lock.unlock() }
    return isSet
  }

  func set() {
    lock.lock()
    isSet = true
    lock.unlock()
  }
}
