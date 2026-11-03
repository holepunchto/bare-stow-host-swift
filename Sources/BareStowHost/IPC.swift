import Foundation

/// A byte transport to a stowed bundle, such as a `Sidecar` from
/// `bare-sidecar-swift`.
public protocol Transport: AsyncSequence, Sendable where Element == Data {
  /// Writes `data` to the bundle.
  func write(_ data: Data) async throws
}

/// A fault reported by or observed in the worker.
///
/// Its `code` tells failures apart: `WORKER_ERROR` for a fault the worker
/// reported, `WORKER_EXITED` with `exitCode` for a worker that exited,
/// `WORKER_KILLED` with `signal` for a worker killed by a signal,
/// `WORKER_CLOSED` for a transport that closed before ready,
/// `WORKER_TIMEOUT` for a worker that timed out before ready, and
/// `TRANSPORT_ERROR` with `underlyingError` for a transport that failed while
/// reading or writing. A transport that is reset or broken by the worker going
/// away counts as closing rather than failing.
public struct WorkerError: Error, Equatable, CustomStringConvertible, Sendable {
  /// The message of the error.
  public let message: String
  /// The kind of the error, such as `WORKER_EXITED`.
  public let code: String
  /// The stack of the error reported by the worker, if any.
  public let stack: String?
  /// The exit code of a worker that exited without saying why.
  public let exitCode: Int32?
  /// The signal that killed a worker that exited without saying why.
  public let signal: Int32?
  /// The error that caused this one, such as the failure of the transport.
  /// Equality ignores it.
  public let underlyingError: (any Error)?

  public init(
    _ message: String,
    code: String = "WORKER_ERROR",
    stack: String? = nil,
    exitCode: Int32? = nil,
    signal: Int32? = nil,
    underlyingError: (any Error)? = nil
  ) {
    self.message = message
    self.code = code
    self.stack = stack
    self.exitCode = exitCode
    self.signal = signal
    self.underlyingError = underlyingError
  }

  public var description: String { message }

  public static func == (lhs: WorkerError, rhs: WorkerError) -> Bool {
    lhs.message == rhs.message && lhs.code == rhs.code && lhs.stack == rhs.stack
      && lhs.exitCode == rhs.exitCode && lhs.signal == rhs.signal
  }
}

/// The host side of a stowed bundle, layering readiness, termination, and exit
/// reporting over a transport.
///
/// Iterating the handle yields the user data written by the worker until the
/// transport ends, and throws a ``WorkerError`` once the worker has reported a
/// fault or exited without saying why. Whoever creates the handle must report
/// the exit of the worker with ``exited(code:signal:)``.
public final class IPC: AsyncSequence, @unchecked Sendable {
  public typealias Element = Data

  /// The default number of seconds a terminating worker gets to stop and exit.
  public static let defaultGrace: TimeInterval = 5

  private let kill: @Sendable () -> Void
  private let grace: TimeInterval
  private let data: AsyncThrowingStream<Data, Error>
  private let dataContinuation: AsyncThrowingStream<Data, Error>.Continuation
  private let writes: AsyncStream<(Data, CheckedContinuation<Void, Error>?)>.Continuation

  private let lock = NSLock()
  private var ready: Result<Void, WorkerError>?
  private var readyWaiters: [CheckedContinuation<Void, Error>] = []
  private var transportEnded = false
  private var closed = false
  private var closeWaiters: [CheckedContinuation<Void, Never>] = []
  private var reportedExitCode: Int32?
  private var exitReported = false
  private var failure: WorkerError?
  private var terminating: Task<Void, Never>?
  private var terminateSent = false
  private var holding = false
  private var transportFailure: WorkerError?
  private var failing: Task<Void, Never>?
  private var timer: Task<Void, Never>?

  /// Wraps the host side of a stowed bundle's `transport` and sends the
  /// `grace` to the worker.
  ///
  /// Whoever creates the handle must report the exit of the worker with
  /// ``exited(code:signal:)``. The end of a transport that ends without the
  /// worker saying why is held back until then, so that the handle fails with
  /// the reason. A transport that fails instead waits `grace` seconds for the
  /// exit, failing with the exit and the `TRANSPORT_ERROR` as its
  /// `underlyingError`, or calling `kill` and failing with the
  /// `TRANSPORT_ERROR` if the worker hasn't exited by then.
  ///
  /// - Parameters:
  ///   - transport: The transport to the bundle.
  ///   - kill: Stops a worker that ignores `terminate` for `grace` seconds.
  ///   - timeout: The number of seconds to wait for the worker to signal
  ///     ready before failing ``ready()`` and terminating it, or `nil` to wait
  ///     indefinitely.
  ///   - grace: The number of seconds a terminating worker gets to stop and
  ///     exit before `kill` is called.
  public init<T: Transport>(
    _ transport: T,
    kill: @escaping @Sendable () -> Void,
    timeout: TimeInterval? = nil,
    grace: TimeInterval = IPC.defaultGrace
  ) {
    self.kill = kill
    self.grace = grace

    (data, dataContinuation) = AsyncThrowingStream.makeStream()

    let (pending, writes) = AsyncStream<(Data, CheckedContinuation<Void, Error>?)>.makeStream()

    self.writes = writes

    Task {
      for await (frame, continuation) in pending {
        do {
          try await transport.write(frame)
          continuation?.resume()
        } catch {
          let error = self.onTransportFailure(error)
          continuation?.resume(throwing: error)
        }
      }
    }

    writes.yield((Frame.control("configure", ["grace": Int(grace * 1000)]), nil))

    Task { await self.readFrames(transport) }

    if let timeout {
      let timer = Task {
        try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))

        if !Task.isCancelled {
          self.settleReady(
            .failure(WorkerError("Worker timed out before ready", code: "WORKER_TIMEOUT")),
            unless: { self.holding }
          )
        }
      }

      locked { self.timer = timer }
    }
  }

  /// The exit code the worker reported, if any.
  public var exitCode: Int32? {
    locked { reportedExitCode }
  }

  /// Waits for the worker to signal ready.
  ///
  /// - Throws: A ``WorkerError`` if the worker errors, exits, closes, or times
  ///   out before then, in which case the worker is terminated.
  public func ready() async throws {
    try await withCheckedThrowingContinuation { continuation in
      let ready: Result<Void, WorkerError>? = locked {
        if self.ready == nil { readyWaiters.append(continuation) }
        return self.ready
      }

      if let ready { continuation.resume(with: ready) }
    }
  }

  public func makeAsyncIterator() -> AsyncThrowingStream<Data, Error>.AsyncIterator {
    data.makeAsyncIterator()
  }

  /// Writes `data` to the worker as user data.
  ///
  /// - Throws: A `WORKER_CLOSED` ``WorkerError`` if the worker has gone away,
  ///   or a `TRANSPORT_ERROR` one if the transport failed.
  public func write(_ data: Data) async throws {
    try await withCheckedThrowingContinuation { continuation in
      writes.yield((Frame.encode(Frame.user, data), continuation))
    }
  }

  /// Waits for the handle to close and returns the exit code the worker
  /// reported, if any.
  @discardableResult
  public func wait() async -> Int32? {
    await waitClosed()

    return exitCode
  }

  /// Asks the worker to terminate and waits for the transport to end, calling
  /// `kill` if the worker hasn't exited within the grace period.
  ///
  /// - Returns: The exit code the worker reported, if any.
  @discardableResult
  public func terminate() async -> Int32? {
    await close()

    return exitCode
  }

  /// Like ``terminate()``, without the exit code.
  public func close() async {
    let terminating = locked {
      if self.terminating == nil {
        self.terminating = Task { await self.runTerminate() }
      }

      return self.terminating!
    }

    await terminating.value
  }

  /// Reports that the worker has exited, as observed by the transport, such
  /// as the process of a sidecar. It must be called once the worker exits, as
  /// a transport that ends or fails without the worker saying why is held
  /// back until then.
  ///
  /// If the worker exited without saying why and was not asked to, the handle
  /// fails with a `WORKER_EXITED` or `WORKER_KILLED` ``WorkerError`` carrying
  /// `exitCode` and `signal`.
  public func exited(code: Int32?, signal: Int32? = nil) {
    let reported: (cause: WorkerError?, failing: Task<Void, Never>?)? = locked {
      if exitReported || terminateSent || failure != nil { return nil }
      defer {
        transportFailure = nil
        failing = nil
      }
      return (transportFailure, failing)
    }

    guard let (cause, failing) = reported else { return }

    failing?.cancel()

    if let signal {
      fail(
        WorkerError(
          "Worker was killed by signal \(signal)",
          code: "WORKER_KILLED",
          exitCode: code,
          signal: signal,
          underlyingError: cause
        )
      )
    } else {
      fail(
        WorkerError(
          "Worker exited with code \(code ?? -1)",
          code: "WORKER_EXITED",
          exitCode: code,
          underlyingError: cause
        )
      )
    }
  }

  private func readFrames<T: Transport>(_ transport: T) async {
    var decoder = FrameDecoder()

    do {
      for try await chunk in transport {
        for (tag, payload) in decoder.feed(chunk) {
          onFrame(tag, payload)
        }
      }
    } catch {
      _ = onTransportFailure(error)
    }

    onTransportEnd()
  }

  private func onFrame(_ tag: UInt8, _ payload: Data) {
    if tag == Frame.user {
      dataContinuation.yield(payload)
      return
    }

    guard tag == Frame.control, let message = decodeControl(payload) else { return }

    switch message["type"] as? String {
    case "ready":
      settleReady(.success(()))
    case "exit":
      let code = (message["code"] as? NSNumber)?.int32Value

      let faulted = locked {
        exitReported = true
        reportedExitCode = code
        return failure != nil
      }

      // An error frame precedes the exit frame of a worker that faulted.
      if !faulted {
        settleReady(
          .failure(
            WorkerError(
              "Worker exited with code \(code ?? -1) before ready",
              code: "WORKER_EXITED",
              exitCode: code
            )
          )
        )
      }
    case "error":
      fail(
        WorkerError(
          message["message"] as? String ?? "",
          code: "WORKER_ERROR",
          stack: message["stack"] as? String
        )
      )
    default:
      break
    }
  }

  private func onTransportEnd() {
    let crashed: (Bool, Task<Void, Never>?) = locked {
      transportEnded = true
      if !mayHaveCrashed { return (false, nil) }
      holding = true
      return (true, timer)
    }

    // The end of a crashed worker is held back until its exit is reported,
    // so that readers fail with the reason rather than seeing a clean end.
    // It can no longer time out, as it already ended.
    if crashed.0 {
      crashed.1?.cancel()
    } else {
      finishClosing()
    }
  }

  /// Returns the error that a write which hit `error` throws.
  private func onTransportFailure(_ error: any Error) -> WorkerError {
    if isPeerGone(error) { return WorkerError("Worker closed", code: "WORKER_CLOSED") }

    let failure = WorkerError(
      String(describing: error), code: "TRANSPORT_ERROR", underlyingError: error)

    enum Action {
      case drop
      case wait(Task<Void, Never>?)
      case waiting
    }

    let action: Action = locked {
      if transportEnded || exitReported || terminateSent || self.failure != nil { return .drop }
      if transportFailure != nil { return .waiting }
      holding = true
      transportFailure = failure
      return .wait(timer)
    }

    switch action {
    case .drop:
      return WorkerError("Worker closed", code: "WORKER_CLOSED")
    case .wait(let timer):
      timer?.cancel()
      waitForExit(failure)
    case .waiting:
      break
    }

    return failure
  }

  private func waitForExit(_ failure: WorkerError) {
    let failing = Task {
      try? await Task.sleep(nanoseconds: UInt64(grace * 1_000_000_000))

      if Task.isCancelled { return }

      let expired = locked {
        if transportFailure == nil || self.failure != nil { return false }
        transportFailure = nil
        self.failure = failure
        return true
      }

      guard expired else { return }

      kill()
      propagate(failure)
    }

    locked {
      if transportFailure != nil { self.failing = failing } else { failing.cancel() }
    }
  }

  private var mayHaveCrashed: Bool {
    !exitReported && !terminateSent && failure == nil
  }

  private func finishClosing() {
    let closing: (WorkerError?, [CheckedContinuation<Void, Never>])? = locked {
      if closed { return nil }
      closed = true
      defer { closeWaiters = [] }
      return (failure, closeWaiters)
    }

    guard let (failure, waiters) = closing else { return }

    if let failure {
      settleReady(.failure(failure))
      dataContinuation.finish(throwing: failure)
    } else {
      settleReady(.failure(WorkerError("Worker closed before ready", code: "WORKER_CLOSED")))
      dataContinuation.finish()
    }

    for waiter in waiters { waiter.resume() }
  }

  private func fail(_ error: WorkerError) {
    let first = locked {
      if failure != nil { return false }
      failure = error
      return true
    }

    if first { propagate(error) }
  }

  private func propagate(_ error: WorkerError) {
    settleReady(.failure(error))
    dataContinuation.finish(throwing: error)

    if locked({ transportEnded }) { finishClosing() }

    Task { await close() }
  }

  private func settleReady(
    _ result: Result<Void, WorkerError>,
    unless skipped: () -> Bool = { false }
  ) {
    let settled: ([CheckedContinuation<Void, Error>], Task<Void, Never>?)? = locked {
      if ready != nil || skipped() { return nil }
      ready = result
      defer { readyWaiters = [] }
      return (readyWaiters, timer)
    }

    guard let (waiters, timer) = settled else { return }

    timer?.cancel()

    for waiter in waiters { waiter.resume(with: result.mapError { $0 }) }

    if case .failure = result {
      Task { await close() }
    }
  }

  private func waitClosed() async {
    await withCheckedContinuation { continuation in
      let closed = locked {
        if !self.closed { closeWaiters.append(continuation) }
        return self.closed
      }

      if closed { continuation.resume() }
    }
  }

  private func runTerminate() async {
    let open = locked {
      if !transportEnded { terminateSent = true }
      return !transportEnded
    }

    guard open else { return await waitClosed() }

    let killing = Task {
      try? await Task.sleep(nanoseconds: UInt64(grace * 1_000_000_000))

      if !Task.isCancelled { kill() }
    }

    writes.yield((Frame.control("terminate"), nil))

    await waitClosed()

    killing.cancel()
  }

  private func isPeerGone(_ error: any Error) -> Bool {
    guard let error = error as? POSIXError else { return false }
    return [.EPIPE, .ECONNRESET, .ECONNABORTED].contains(error.code)
  }

  private func locked<R>(_ body: () -> R) -> R {
    lock.lock()
    defer { lock.unlock() }
    return body()
  }
}
