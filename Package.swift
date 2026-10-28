// swift-tools-version: 5.10
import PackageDescription

let package = Package(
  name: "BareStowHost",
  platforms: [.macOS(.v13), .iOS(.v15)],
  products: [
    .library(name: "BareStowHost", targets: ["BareStowHost"])
  ],
  targets: [
    .target(name: "BareStowHost"),
    .testTarget(name: "BareStowHostTests", dependencies: ["BareStowHost"]),
  ]
)
