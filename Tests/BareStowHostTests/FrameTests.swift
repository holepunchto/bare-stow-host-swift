import BareStowHost
import Foundation
import XCTest

final class FrameTests: XCTestCase {
  // Written by the JavaScript bare-stow-protocol for the same frames.
  func testEncodesFramesLikeTheJavaScriptImplementation() {
    XCTAssertEqual(
      hex(Frame.control("configure", ["grace": 5000])),
      "22000000007b2274797065223a22636f6e666967757265222c226772616365223a353030307d"
    )
    XCTAssertEqual(
      hex(Frame.control("ready")),
      "11000000007b2274797065223a227265616479227d"
    )
    XCTAssertEqual(
      hex(Frame.encode(Frame.user, Data("hello".utf8))),
      "060000000168656c6c6f"
    )
  }

  func testDecodesFramesSplitAcrossChunks() {
    var decoder = FrameDecoder()
    let data = Frame.encode(Frame.user, Data("hello".utf8)) + Frame.control("ready")

    var frames: [(tag: UInt8, payload: Data)] = []

    for byte in data {
      frames += decoder.feed(Data([byte]))
    }

    XCTAssertEqual(frames.map(\.tag), [Frame.user, Frame.control])
    XCTAssertEqual(frames.map(\.payload), [Data("hello".utf8), Data(#"{"type":"ready"}"#.utf8)])
  }

  func testDecodesSeveralFramesFromOneChunk() {
    var decoder = FrameDecoder()
    let data =
      Frame.encode(Frame.user, Data("a".utf8)) + Frame.encode(Frame.user, Data())
      + Frame.encode(Frame.user, Data("b".utf8))

    XCTAssertEqual(decoder.feed(data).map(\.payload), [Data("a".utf8), Data(), Data("b".utf8)])
  }

  func testSkipsEmptyFrames() {
    var decoder = FrameDecoder()
    let data = Data([0, 0, 0, 0]) + Frame.encode(Frame.user, Data("a".utf8))

    XCTAssertEqual(decoder.feed(data).map(\.payload), [Data("a".utf8)])
  }

  func testIgnoresInvalidControlPayloads() {
    XCTAssertNil(decodeControl(Data("not json".utf8)))
    XCTAssertNil(decodeControl(Data("[1, 2]".utf8)))
    XCTAssertEqual(decodeControl(Data(#"{"type":"ready"}"#.utf8))?["type"] as? String, "ready")
  }

  private func hex(_ data: Data) -> String {
    data.map { String(format: "%02x", $0) }.joined()
  }
}
