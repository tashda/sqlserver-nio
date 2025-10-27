@testable import SQLServerKit
import XCTest

final class SQLServerClassificationDecodeTests: XCTestCase {
    func testDecodeSensitivityClassification() {
        // Build: labels=1 {name:'Labl', id:'L1'}; infos=1 {name:'PII', id:'P1'}; rank=2; columns=1; prop=1 with idx 0,0, rank=2
        var bytes: [UInt8] = []
        func appendU16(_ v: UInt16) { bytes += withUnsafeBytes(of: v.littleEndian, Array.init) }
        func appendU32(_ v: UInt32) { bytes += withUnsafeBytes(of: v.littleEndian, Array.init) }
        func appendBVar(_ s: String) { let u = Array(s.utf16); bytes.append(UInt8(u.count)); u.forEach { bytes += withUnsafeBytes(of: $0.littleEndian, Array.init) } }
        appendU16(1)
        appendBVar("Labl"); appendBVar("L1")
        appendU16(1)
        appendBVar("PII"); appendBVar("P1")
        appendU32(2) // global rank
        appendU16(1) // column count
        appendU16(1) // properties per column
        appendU16(0) // label index
        appendU16(0) // info index
        appendU32(2) // rank

        // Decode directly via internal helper
        guard let classification = SQLServerSessionAndClassificationDecoder.decodeSensitivityClassification(from: bytes) else {
            XCTFail("Failed to decode classification payload")
            return
        }

        XCTAssertEqual(classification.labels.count, 1)
        XCTAssertEqual(classification.labels.first?.name, "Labl")
        XCTAssertEqual(classification.labels.first?.id, "L1")

        XCTAssertEqual(classification.informationTypes.count, 1)
        XCTAssertEqual(classification.informationTypes.first?.name, "PII")
        XCTAssertEqual(classification.informationTypes.first?.id, "P1")

        XCTAssertEqual(classification.rank, .high)
        XCTAssertEqual(classification.columns.count, 1)
        let props = classification.columns.first?.properties ?? []
        XCTAssertEqual(props.count, 1)
        XCTAssertEqual(props.first?.label?.name, "Labl")
        XCTAssertEqual(props.first?.informationType?.name, "PII")
        XCTAssertEqual(props.first?.rank, .high)
    }
}
