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

    worker.close()
    await ipc.wait()
  }

  func testSendsTheGraceToTheWorker() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, grace: 2.5)
    let worker = Worker(right)

    let message = await worker.receive("configure")

    XCTAssertEqual(message?["grace"] as? Int, 2500)

    worker.close()
    await ipc.wait()
  }

  func testFailsReadyWhenTheWorkerErrorsBeforeReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.sendError("boom")

    await assertReadyFails(ipc, "boom")

    _ = await worker.receive("terminate")
    worker.close()
  }

  func testFailsReadyWhenTheWorkerExitsBeforeReady() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() })
    let worker = Worker(right)

    try await worker.send("exit", ["code": 1])

    await assertReadyFails(ipc, "Worker exited with code 1 before ready")

    _ = await worker.receive("terminate")
    worker.close()
  }

  func testFailsReadyWhenTheTransportClosesBeforeReady() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.close()

    await assertReadyFails(ipc, "Worker closed before ready")
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

  func testWaitReturnsWhenTheTransportCloses() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.close()

    let code = await ipc.wait()

    XCTAssertNil(code)
  }

  func testFailsReadyAndTerminatesTheWorkerOnTimeout() async throws {
    let (left, right) = pair()
    let ipc = IPC(left, kill: { left.close() }, timeout: 0.01)
    let worker = Worker(right)

    await assertReadyFails(ipc, "Worker timed out before ready")

    _ = await worker.receive("terminate")
    worker.close()
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

  func testTerminateReturnsOnceTheTransportHasClosed() async throws {
    let (left, _) = pair()
    let ipc = IPC(left, kill: { left.close() })

    left.close()

    await assertReadyFails(ipc, "Worker closed before ready")

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
      XCTAssertEqual(error.message, "Worker exited with code 3")
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

    ipc.exited(code: nil, signal: 9)

    do {
      for try await _ in ipc {}
      XCTFail("iteration succeeded")
    } catch let error as WorkerError {
      XCTAssertEqual(error.message, "Worker was killed by signal 9")
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

  private func assertReadyFails(
    _ ipc: IPC,
    _ message: String,
    file: StaticString = #filePath,
    line: UInt = #line
  ) async {
    do {
      try await ipc.ready()
      XCTFail("ready succeeded", file: file, line: line)
    } catch {
      XCTAssertEqual((error as? WorkerError)?.message, message, file: file, line: line)
    }
  }
}
