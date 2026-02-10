// Example demonstrating the new ParquetStream API for streaming Parquet generation
/* global process */

import { ParquetStream } from '../src/parquet-stream.js'
import { schemaFromColumnData } from '../src/schema.js'

// Example 1: Basic usage with all data at once
function example1() {
  const columnData = [
    { name: 'id', data: [1, 2, 3, 4, 5] },
    { name: 'name', data: ['Alice', 'Bob', 'Charlie', 'David', 'Eve'] },
    { name: 'age', data: [25, 30, 35, 40, 45] },
  ]

  const schema = schemaFromColumnData({ columnData })
  const stream = new ParquetStream({ schema })

  // Create Parquet file in chunks
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

  console.log(`Example 1: Created Parquet file of ${buffer.byteLength} bytes`)
  return buffer
}

// Example 2: Streaming with multiple row groups
function example2() {
  // Prepare data in batches
  const batch1 = [
    { name: 'id', data: [1, 2, 3] },
    { name: 'value', data: [10.5, 20.3, 30.7] },
  ]

  const batch2 = [
    { name: 'id', data: [4, 5, 6] },
    { name: 'value', data: [40.2, 50.9, 60.1] },
  ]

  const batch3 = [
    { name: 'id', data: [7, 8, 9] },
    { name: 'value', data: [70.4, 80.6, 90.8] },
  ]

  const schema = schemaFromColumnData({ columnData: batch1 })
  const stream = new ParquetStream({ schema })

  const chunks = [
    stream.createHeaderBytes(),
    stream.createRowGroupBytes(batch1),
    stream.createRowGroupBytes(batch2),
    stream.createRowGroupBytes(batch3),
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

  console.log(`Example 2: Created Parquet file with ${stream.row_groups.length} row groups (${buffer.byteLength} bytes)`)
  console.log(`  Total rows: ${stream.num_rows}`)
  return buffer
}

// Example 3: Custom options
function example3() {
  const columnData = [
    { name: 'timestamp', data: [Date.now(), Date.now() + 1000, Date.now() + 2000] },
    { name: 'message', data: ['Started', 'Processing', 'Completed'] },
  ]

  const schema = schemaFromColumnData({ columnData })

  // Custom options
  const stream = new ParquetStream({
    schema,
    codec: 'UNCOMPRESSED', // Use uncompressed data
    statistics: false, // Disable statistics
    kvMetadata: [
      { key: 'application', value: 'my-app' },
      { key: 'version', value: '1.0.0' },
    ],
  })

  const chunks = [
    stream.createHeaderBytes(),
    stream.createRowGroupBytes(columnData, { pageSize: 512 * 1024 }), // 512KB pages
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

  console.log(`Example 3: Created Parquet file with custom options (${buffer.byteLength} bytes)`)
  return buffer
}

// Example 4: Streaming to a file (Node.js)
async function example4() {
  const fs = await import('fs')

  const columnData = [
    { name: 'id', data: [1, 2, 3, 4, 5] },
    { name: 'value', data: ['a', 'b', 'c', 'd', 'e'] },
  ]

  const schema = schemaFromColumnData({ columnData })
  const stream = new ParquetStream({ schema })

  const filename = '/tmp/stream-example.parquet'

  // Write chunks directly to file as they're created
  fs.writeFileSync(filename, stream.createHeaderBytes())
  fs.appendFileSync(filename, stream.createRowGroupBytes(columnData))
  fs.appendFileSync(filename, stream.createFooterBytes())

  const stats = fs.statSync(filename)
  console.log(`Example 4: Created file ${filename} (${stats.size} bytes)`)
  return filename
}

// Run examples
if (import.meta.url === `file://${process.argv[1]}`) {
  console.log('ParquetStream API Examples\n')
  example1()
  console.log()
  example2()
  console.log()
  example3()
  console.log()
  await example4()
}

export { example1, example2, example3, example4 }
