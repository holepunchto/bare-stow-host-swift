import BareStowHost
import Foundation

final class End: Transport, @unchecked Sendable {
  typealias Element = Data

  private let data: AsyncThrowingStream<Data, Error>
  private let continuation: AsyncThrowingStream<Data, Error>.Continuation
  private let lock = NSLock()
  private var isClosed = false
  private let writeFailure: Error?
  private var isEnded = false
  private var endWaiters: [CheckedContinuation<Void, Never>] = []

  weak var peer: End?

  init(failingWritesWith writeFailure: Error? = nil) {
    (data, continuation) = AsyncThrowingStream.makeStream()
    self.writeFailure = writeFailure
  }

  var closed: Bool {
    lock.lock()
    defer { lock.unlock() }
    return isClosed
  }

  func makeAsyncIterator() -> Iterator {
    Iterator(base: data.makeAsyncIterator(), end: self)
  }

  struct Iterator: AsyncIteratorProtocol {
    var base: AsyncThrowingStream<Data, Error>.AsyncIterator
    let end: End

    mutating func next() async throws -> Data? {
      do {
        let data = try await base.next()
        if data == nil { end.markEnded() }
        return data
      } catch {
        end.markEnded()
        throw error
      }
    }
  }

  func ended() async {
    await withCheckedContinuation { continuation in
      lock.lock()
      let ended = isEnded
      if !ended { endWaiters.append(continuation) }
      lock.unlock()

      if ended { continuation.resume() }
    }
  }

  private func markEnded() {
    lock.lock()
    isEnded = true
    let waiters = endWaiters
    endWaiters = []
    lock.unlock()

    for waiter in waiters { waiter.resume() }
  }

  func write(_ data: Data) async throws {
    if let writeFailure { throw writeFailure }

    guard !closed, let peer else { throw POSIXError(.EPIPE) }

    peer.continuation.yield(data)
  }

  func fail(_ error: Error) {
    continuation.finish(throwing: error)
  }

  func close() {
    for end in [self, peer].compactMap({ $0 }) {
      end.lock.lock()
      end.isClosed = true
      end.lock.unlock()
      end.continuation.finish()
    }
  }
}

func pair(failingWritesWith writeFailure: Error? = nil) -> (End, End) {
  let a = End(failingWritesWith: writeFailure)
  let b = End()
  a.peer = b
  b.peer = a
  return (a, b)
}

final class Worker: @unchecked Sendable {
  let end: End

  private var iterator: AsyncStream<(UInt8, Data)>.AsyncIterator

  init(_ end: End) {
    self.end = end

    let (frames, continuation) = AsyncStream<(UInt8, Data)>.makeStream()

    self.iterator = frames.makeAsyncIterator()

    Task {
      var decoder = FrameDecoder()

      do {
        for try await chunk in end {
          for frame in decoder.feed(chunk) { continuation.yield(frame) }
        }
      } catch {}

      continuation.finish()
    }
  }

  func send(_ type: String, _ fields: KeyValuePairs<String, Int> = [:]) async throws {
    try await end.write(Frame.control(type, fields))
  }

  func sendError(_ message: String) async throws {
    let json = #"{"type":"error","message":"\#(message)"}"#

    try await end.write(Frame.encode(Frame.control, Data(json.utf8)))
  }

  func write(_ data: Data) async throws {
    try await end.write(Frame.encode(Frame.user, data))
  }

  func receive(_ type: String) async -> [String: Any]? {
    while let (tag, payload) = await iterator.next() {
      if tag == Frame.control, let message = decodeControl(payload),
        message["type"] as? String == type
      {
        return message
      }
    }

    return nil
  }

  func data() async -> Data? {
    while let (tag, payload) = await iterator.next() {
      if tag == Frame.user { return payload }
    }

    return nil
  }

  func close() {
    end.close()
  }
}
