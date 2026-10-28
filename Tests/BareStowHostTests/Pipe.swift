import BareStowHost
import Foundation

final class End: Transport, @unchecked Sendable {
  typealias Element = Data

  private let data: AsyncStream<Data>
  private let continuation: AsyncStream<Data>.Continuation
  private let lock = NSLock()
  private var isClosed = false

  weak var peer: End?

  init() {
    (data, continuation) = AsyncStream.makeStream()
  }

  var closed: Bool {
    lock.lock()
    defer { lock.unlock() }
    return isClosed
  }

  func makeAsyncIterator() -> AsyncStream<Data>.AsyncIterator {
    data.makeAsyncIterator()
  }

  func write(_ data: Data) async throws {
    guard !closed, let peer else { throw POSIXError(.EPIPE) }

    peer.continuation.yield(data)
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

func pair() -> (End, End) {
  let a = End()
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

      for await chunk in end {
        for frame in decoder.feed(chunk) { continuation.yield(frame) }
      }

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
