import { ParquetStream } from './parquet-stream.js'
import { schemaFromColumnData } from './schema.js'

/**
 * Write data as parquet to a file or stream.
 *
 * @import {ParquetWriteOptions} from '../src/types.js'
 * @param {ParquetWriteOptions} options
 */
export function parquetWrite({
  writer,
  columnData,
  schema,
  codec = 'SNAPPY',
  compressors,
  statistics = true,
  rowGroupSize = [1000, 100000],
  kvMetadata,
  pageSize = 1048576,
}) {
  if (!schema) {
    schema = schemaFromColumnData({ columnData })
  } else if (columnData.some(({ type }) => type)) {
    throw new Error('cannot provide both schema and columnData type')
  } else {
    // TODO: validate schema
  }

  const stream = new ParquetStream({
    schema,
    codec,
    compressors,
    statistics,
    kvMetadata,
  })

  // Write header
  const headerBytes = stream.createHeaderBytes()
  writer.appendBytes(headerBytes)

  // Write row groups
  const columnDataRows = columnData[0]?.data?.length || 0
  for (const { groupStartIndex, groupSize } of groupIterator({ columnDataRows, rowGroupSize })) {
    const groupData = columnData.map(col => ({
      ...col,
      data: col.data.slice(groupStartIndex, groupStartIndex + groupSize),
    }))
    const rowGroupBytes = stream.createRowGroupBytes(groupData, { pageSize })
    writer.appendBytes(rowGroupBytes)
  }

  // Write footer
  const footerBytes = stream.createFooterBytes()
  writer.appendBytes(footerBytes)

  writer.finish()
}

/**
 * Write data as parquet to an ArrayBuffer.
 *
 * @param {Omit<ParquetWriteOptions, 'writer'>} options
 * @returns {ArrayBuffer}
 */
export function parquetWriteBuffer(options) {
  const { columnData, schema, codec = 'SNAPPY', compressors, statistics = true, rowGroupSize = [1000, 100000], kvMetadata, pageSize = 1048576 } = options

  let finalSchema = schema
  if (!finalSchema) {
    finalSchema = schemaFromColumnData({ columnData })
  } else if (columnData.some(({ type }) => type)) {
    throw new Error('cannot provide both schema and columnData type')
  }

  const stream = new ParquetStream({
    schema: finalSchema,
    codec,
    compressors,
    statistics,
    kvMetadata,
  })

  // Collect all chunks
  const chunks = []

  // Header
  chunks.push(stream.createHeaderBytes())

  // Row groups
  const columnDataRows = columnData[0]?.data?.length || 0
  for (const { groupStartIndex, groupSize } of groupIterator({ columnDataRows, rowGroupSize })) {
    const groupData = columnData.map(col => ({
      ...col,
      data: col.data.slice(groupStartIndex, groupStartIndex + groupSize),
    }))
    chunks.push(stream.createRowGroupBytes(groupData, { pageSize }))
  }

  // Footer
  chunks.push(stream.createFooterBytes())

  // Combine all chunks into a single ArrayBuffer
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
  const result = new ArrayBuffer(totalLength)
  const resultView = new Uint8Array(result)
  let offset = 0
  for (const chunk of chunks) {
    resultView.set(chunk, offset)
    offset += chunk.byteLength
  }

  return result
}

/**
 * Create an iterator for row groups based on the specified row group size.
 * If rowGroupSize is an array, it will return groups based on the sizes in the array.
 * When the array runs out, it will continue with the last size.
 *
 * @param {object} options
 * @param {number} options.columnDataRows - Total number of rows in the column data
 * @param {number | number[]} options.rowGroupSize - Size of each row group or an array of sizes
 * @returns {Array<{groupStartIndex: number, groupSize: number}>}
 */
function groupIterator({ columnDataRows, rowGroupSize }) {
  if (Array.isArray(rowGroupSize) && !rowGroupSize.length) {
    throw new Error('rowGroupSize array cannot be empty')
  }
  const groups = []
  let groupIndex = 0
  let groupStartIndex = 0
  while (groupStartIndex < columnDataRows) {
    const size = Array.isArray(rowGroupSize)
      ? rowGroupSize[Math.min(groupIndex, rowGroupSize.length - 1)]
      : rowGroupSize
    const groupSize = Math.min(size, columnDataRows - groupStartIndex)
    groups.push({ groupStartIndex, groupSize })
    groupStartIndex += size
    groupIndex++
  }
  return groups
}
