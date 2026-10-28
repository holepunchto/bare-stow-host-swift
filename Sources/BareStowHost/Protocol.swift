import Foundation

/// The framing of the control and user data channels, wire-compatible with the
/// JavaScript `bare-stow-protocol`.
public enum Frame {
  /// The tag of a control frame.
  public static let control: UInt8 = 0x00
  /// The tag of a user data frame.
  public static let user: UInt8 = 0x01

  /// Encodes a frame of `tag` carrying `payload`.
  public static func encode(_ tag: UInt8, _ payload: Data) -> Data {
    var frame = Data(capacity: 5 + payload.count)

    withUnsafeBytes(of: UInt32(payload.count + 1).littleEndian) { frame.append(contentsOf: $0) }

    frame.append(tag)
    frame.append(payload)

    return frame
  }

  /// Encodes a control frame of `type` with integer `fields`, in order.
  public static func control(_ type: String, _ fields: KeyValuePairs<String, Int> = [:]) -> Data {
    var json = #"{"type":"# + quote(type)

    for (key, value) in fields {
      json += "," + quote(key) + ":" + String(value)
    }

    return encode(control, Data((json + "}").utf8))
  }

  private static func quote(_ string: String) -> String {
    let data = try! JSONSerialization.data(withJSONObject: [string], options: [])

    return String(decoding: data.dropFirst().dropLast(), as: UTF8.self)
  }
}

/// Decodes frames from a stream of bytes.
public struct FrameDecoder {
  private var buffer = Data()

  public init() {}

  /// Feeds `data` to the decoder and returns the frames it completes, as their
  /// tag and payload.
  public mutating func feed(_ data: Data) -> [(tag: UInt8, payload: Data)] {
    buffer.append(data)

    var frames: [(tag: UInt8, payload: Data)] = []
    var offset = buffer.startIndex

    while buffer.endIndex - offset >= 4 {
      let length = buffer[offset..<offset + 4].reduce(into: 0) { result, byte in
        result = result >> 8 | Int(byte) << 24
      }

      let start = offset + 4
      let end = start + length

      guard end <= buffer.endIndex else { break }

      if length > 0 {
        frames.append((buffer[start], Data(buffer[start + 1..<end])))
      }

      offset = end
    }

    if offset == buffer.endIndex {
      buffer.removeAll(keepingCapacity: true)
    } else if offset > buffer.startIndex {
      buffer.removeSubrange(..<offset)
    }

    return frames
  }
}

/// Decodes the JSON object of a control frame, or returns `nil` if it isn't
/// one.
public func decodeControl(_ payload: Data) -> [String: Any]? {
  try? JSONSerialization.jsonObject(with: payload) as? [String: Any]
}
