import Foundation

/// A byte transport to a stowed bundle, such as a `Sidecar` from
/// `bare-sidecar-swift`.
public protocol Transport: AsyncSequence, Sendable where Element == Data {
  /// Writes `data` to the bundle.
  func write(_ data: Data) async throws
}

/// A fault reported by or observed in the worker.
public struct WorkerError: Error, Equatable, CustomStringConvertible, Sendable {
  /// The message of the error.
  public let message: String
  /// The stack of the error reported by the worker, if any.
  public let stack: String?
  /// The exit code of a worker that exited without saying why.
  public let exitCode: Int32?
  /// The signal that killed a worker that exited without saying why.
  public let signal: Int32?

  public init(
    _ message: String,
    stack: String? = nil,
    exitCode: Int32? = nil,
    signal: Int32? = nil
  ) {
    self.message = message
    self.stack = stack
    self.exitCode = exitCode
    self.signal = signal
  }

  public var description: String { message }
}

/// The host side of a stowed bundle, layering readiness, termination, and exit
/// reporting over a transport.
///
/// Iterating the handle yields the user data written by the worker until the
/// transport ends, and throws a ``WorkerError`` once the worker has reported a
/// fault or exited without saying why.
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
  private var closed = false
  private var reading: Task<Void, Never>!
  private var reportedExitCode: Int32?
  private var exitReported = false
  private var failure: WorkerError?
  private var terminating: Task<Void, Never>?
  private var timer: Task<Void, Never>?

  /// Wraps the host side of a stowed bundle's `transport` and sends the
  /// `grace` to the worker.
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
          continuation?.resume(throwing: error)
        }
      }
    }

    writes.yield((Frame.control("configure", ["grace": Int(grace * 1000)]), nil))

    reading = Task { await self.readFrames(transport) }

    if let timeout {
      let timer = Task {
        try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))

        if !Task.isCancelled {
          self.settleReady(.failure(WorkerError("Worker timed out before ready")))
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
  public func write(_ data: Data) async throws {
    try await withCheckedThrowingContinuation { continuation in
      writes.yield((Frame.encode(Frame.user, data), continuation))
    }
  }

  /// Waits for the transport to end and returns the exit code the worker
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
  /// as the process of a sidecar.
  ///
  /// If the worker exited without saying why and was not asked to, the handle
  /// fails with a ``WorkerError`` carrying `exitCode` and `signal`.
  public func exited(code: Int32?, signal: Int32? = nil) {
    let ignored = locked { exitReported || terminating != nil || failure != nil }

    if ignored { return }

    if let signal {
      fail(WorkerError("Worker was killed by signal \(signal)", signal: signal))
    } else {
      fail(WorkerError("Worker exited with code \(code ?? -1)", exitCode: code))
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
      fail(WorkerError(String(describing: error)))
    }

    onClose()
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
        settleReady(.failure(WorkerError("Worker exited with code \(code ?? -1) before ready")))
      }
    case "error":
      fail(WorkerError(message["message"] as? String ?? "", stack: message["stack"] as? String))
    default:
      break
    }
  }

  private func onClose() {
    locked { closed = true }

    settleReady(.failure(WorkerError("Worker closed before ready")))
    dataContinuation.finish()
  }

  private func fail(_ error: WorkerError) {
    let first = locked {
      if failure != nil { return false }
      failure = error
      return true
    }

    guard first else { return }

    settleReady(.failure(error))
    dataContinuation.finish(throwing: error)

    Task { await close() }
  }

  private func settleReady(_ result: Result<Void, WorkerError>) {
    let settled: ([CheckedContinuation<Void, Error>], Task<Void, Never>?)? = locked {
      if ready != nil { return nil }
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
    await reading.value
  }

  private func runTerminate() async {
    if locked({ closed }) { return }

    let killing = Task {
      try? await Task.sleep(nanoseconds: UInt64(grace * 1_000_000_000))

      if !Task.isCancelled { kill() }
    }

    writes.yield((Frame.control("terminate"), nil))

    await waitClosed()

    killing.cancel()
  }

  private func locked<R>(_ body: () -> R) -> R {
    lock.lock()
    defer { lock.unlock() }
    return body()
  }
}
