import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { ParquetStream } from '../src/parquet-stream.js'
import { schemaFromColumnData } from '../src/schema.js'

describe('ParquetStream', () => {
  it('creates header bytes', () => {
    const schema = [
      { name: 'root', num_children: 1 },
      { name: 'col', type: 'INT32', repetition_type: 'OPTIONAL' },
    ]
    const stream = new ParquetStream({ schema })
    const headerBytes = stream.createHeaderBytes()

    expect(headerBytes).toBeInstanceOf(Uint8Array)
    expect(headerBytes.byteLength).toBe(4)
    // Check PAR1 magic
    expect(headerBytes[0]).toBe(0x50) // P
    expect(headerBytes[1]).toBe(0x41) // A
    expect(headerBytes[2]).toBe(0x52) // R
    expect(headerBytes[3]).toBe(0x31) // 1
  })

  it('creates row group bytes', () => {
    const columnData = [{ name: 'int', data: [1, 2, 3, 4] }]
    const schema = schemaFromColumnData({ columnData })
    const stream = new ParquetStream({ schema })

    stream.createHeaderBytes() // Initialize offset
    const rowGroupBytes = stream.createRowGroupBytes(columnData)

    expect(rowGroupBytes).toBeInstanceOf(Uint8Array)
    expect(rowGroupBytes.byteLength).toBeGreaterThan(0)
    expect(stream.num_rows).toBe(4n)
    expect(stream.row_groups.length).toBe(1)
  })

  it('creates footer bytes', () => {
    const columnData = [{ name: 'int', data: [1, 2, 3, 4] }]
    const schema = schemaFromColumnData({ columnData })
    const stream = new ParquetStream({ schema })

    stream.createHeaderBytes()
    stream.createRowGroupBytes(columnData)
    const footerBytes = stream.createFooterBytes()

    expect(footerBytes).toBeInstanceOf(Uint8Array)
    expect(footerBytes.byteLength).toBeGreaterThan(0)
    // Check trailing PAR1 magic at end
    expect(footerBytes[footerBytes.length - 4]).toBe(0x50) // P
    expect(footerBytes[footerBytes.length - 3]).toBe(0x41) // A
    expect(footerBytes[footerBytes.length - 2]).toBe(0x52) // R
    expect(footerBytes[footerBytes.length - 1]).toBe(0x31) // 1
  })

  it('creates complete parquet file', async () => {
    const columnData = [
      { name: 'bool', data: [true, false, true, false] },
      { name: 'int', data: [0, 127, 0x7fff, 0x7fffffff] },
      { name: 'string', data: ['a', 'b', 'c', 'd'] },
    ]
    const schema = schemaFromColumnData({ columnData })
    const stream = new ParquetStream({ schema })

    // Collect all chunks
    const chunks = [
      stream.createHeaderBytes(),
      stream.createRowGroupBytes(columnData),
      stream.createFooterBytes(),
    ]

    // Combine chunks into single buffer
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
    const buffer = new ArrayBuffer(totalLength)
    const view = new Uint8Array(buffer)
    let offset = 0
    for (const chunk of chunks) {
      view.set(chunk, offset)
      offset += chunk.byteLength
    }

    // Verify it's a valid parquet file
    const metadata = parquetMetadata(buffer)
    expect(metadata.num_rows).toBe(4n)
    expect(metadata.row_groups.length).toBe(1)
    expect(metadata.row_groups[0].num_rows).toBe(4n)

    // Verify data round-trips correctly
    const result = await parquetReadObjects({ file: buffer })
    expect(result).toEqual([
      { bool: true, int: 0, string: 'a' },
      { bool: false, int: 127, string: 'b' },
      { bool: true, int: 0x7fff, string: 'c' },
      { bool: false, int: 0x7fffffff, string: 'd' },
    ])
  })

  it('creates multiple row groups', async () => {
    const columnData1 = [{ name: 'int', data: [1, 2, 3] }]
    const columnData2 = [{ name: 'int', data: [4, 5, 6] }]
    const columnData3 = [{ name: 'int', data: [7, 8, 9] }]

    const schema = schemaFromColumnData({ columnData: columnData1 })
    const stream = new ParquetStream({ schema })

    // Collect all chunks
    const chunks = [
      stream.createHeaderBytes(),
      stream.createRowGroupBytes(columnData1),
      stream.createRowGroupBytes(columnData2),
      stream.createRowGroupBytes(columnData3),
      stream.createFooterBytes(),
    ]

    // Combine chunks
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
    const buffer = new ArrayBuffer(totalLength)
    const view = new Uint8Array(buffer)
    let offset = 0
    for (const chunk of chunks) {
      view.set(chunk, offset)
      offset += chunk.byteLength
    }

    // Verify
    const metadata = parquetMetadata(buffer)
    expect(metadata.num_rows).toBe(9n)
    expect(metadata.row_groups.length).toBe(3)
    expect(metadata.row_groups[0].num_rows).toBe(3n)
    expect(metadata.row_groups[1].num_rows).toBe(3n)
    expect(metadata.row_groups[2].num_rows).toBe(3n)

    const result = await parquetReadObjects({ file: buffer })
    expect(result).toEqual([
      { int: 1 }, { int: 2 }, { int: 3 },
      { int: 4 }, { int: 5 }, { int: 6 },
      { int: 7 }, { int: 8 }, { int: 9 },
    ])
  })

  it('maintains correct offsets across chunks', async () => {
    const columnData = [{ name: 'string', data: ['test'.repeat(100)] }]
    const schema = schemaFromColumnData({ columnData })
    const stream = new ParquetStream({ schema })

    const header = stream.createHeaderBytes()
    const headerOffset = stream.offset
    expect(headerOffset).toBe(4) // PAR1 magic

    const rowGroup = stream.createRowGroupBytes(columnData)
    const rowGroupOffset = stream.offset
    expect(rowGroupOffset).toBe(headerOffset + rowGroup.byteLength)

    const footer = stream.createFooterBytes()
    const finalOffset = stream.offset
    expect(finalOffset).toBe(rowGroupOffset + footer.byteLength)

    // Combine and verify
    const totalLength = header.byteLength + rowGroup.byteLength + footer.byteLength
    const buffer = new ArrayBuffer(totalLength)
    const view = new Uint8Array(buffer)
    view.set(header, 0)
    view.set(rowGroup, header.byteLength)
    view.set(footer, header.byteLength + rowGroup.byteLength)

    // Should be able to read the file
    const result = await parquetReadObjects({ file: buffer })
    expect(result.length).toBe(1)
    expect(result[0].string).toBe('test'.repeat(100))
  })

  it('supports custom codec and compressors', () => {
    const columnData = [{ name: 'int', data: [1, 2, 3] }]
    const schema = schemaFromColumnData({ columnData })
    const stream = new ParquetStream({
      schema,
      codec: 'UNCOMPRESSED',
    })

    const chunks = [
      stream.createHeaderBytes(),
      stream.createRowGroupBytes(columnData),
      stream.createFooterBytes(),
    ]

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
    const buffer = new ArrayBuffer(totalLength)
    const view = new Uint8Array(buffer)
    let offset = 0
    for (const chunk of chunks) {
      view.set(chunk, offset)
      offset += chunk.byteLength
    }

    const metadata = parquetMetadata(buffer)
    expect(metadata.row_groups[0].columns[0].meta_data?.codec).toBe('UNCOMPRESSED')
  })

  it('supports statistics and kvMetadata', () => {
    const columnData = [{ name: 'int', data: [1, 2, 3] }]
    const schema = schemaFromColumnData({ columnData })
    const kvMetadata = [
      { key: 'test_key', value: 'test_value' },
      { key: 'another_key', value: 'another_value' },
    ]
    const stream = new ParquetStream({
      schema,
      statistics: true,
      kvMetadata,
    })

    const chunks = [
      stream.createHeaderBytes(),
      stream.createRowGroupBytes(columnData),
      stream.createFooterBytes(),
    ]

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
    const buffer = new ArrayBuffer(totalLength)
    const view = new Uint8Array(buffer)
    let offset = 0
    for (const chunk of chunks) {
      view.set(chunk, offset)
      offset += chunk.byteLength
    }

    const metadata = parquetMetadata(buffer)
    expect(metadata.key_value_metadata).toEqual(kvMetadata)
    expect(metadata.row_groups[0].columns[0].meta_data?.statistics).toBeDefined()
  })

  it('handles complex nested types', async () => {
    const columnData = [
      { name: 'list', data: [[1, 2, 3], [4, 5, 6]] },
      { name: 'obj', data: [{ a: 1, b: 2 }, { a: 3, b: 4 }] },
    ]
    const schema = schemaFromColumnData({ columnData })
    const stream = new ParquetStream({ schema })

    const chunks = [
      stream.createHeaderBytes(),
      stream.createRowGroupBytes(columnData),
      stream.createFooterBytes(),
    ]

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
    const buffer = new ArrayBuffer(totalLength)
    const view = new Uint8Array(buffer)
    let offset = 0
    for (const chunk of chunks) {
      view.set(chunk, offset)
      offset += chunk.byteLength
    }

    const result = await parquetReadObjects({ file: buffer })
    expect(result).toEqual([
      { list: [1, 2, 3], obj: { a: 1, b: 2 } },
      { list: [4, 5, 6], obj: { a: 3, b: 4 } },
    ])
  })
})
