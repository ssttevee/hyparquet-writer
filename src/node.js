import fs from 'fs'
import { ByteWriter } from './bytewriter.js'
import { ParquetStream } from './parquet-stream.js'
import { schemaFromColumnData } from './schema.js'
import { groupIterator } from './util.js'

export * from './index.js'

/**
 * Write data as parquet to a local file.
 *
 * @import {ParquetWriteOptions, Writer} from '../src/types.js'
 * @param {Omit<ParquetWriteOptions, 'writer'> & {filename: string}} options
 */
export function parquetWriteFile(options) {
  const {
    filename,
    columnData,
    schema,
    codec = 'SNAPPY',
    compressors,
    statistics = true,
    rowGroupSize = [1000, 100000],
    kvMetadata,
    pageSize = 1048576,
  } = options

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

  // Create a new file or overwrite existing one
  fs.writeFileSync(filename, '', { flag: 'w' })

  // Write header
  fs.writeFileSync(filename, stream.createHeaderBytes(), { flag: 'a' })

  // Write row groups
  const columnDataRows = columnData[0]?.data?.length || 0
  for (const { groupStartIndex, groupSize } of groupIterator({ columnDataRows, rowGroupSize })) {
    const groupData = columnData.map(col => ({
      ...col,
      data: col.data.slice(groupStartIndex, groupStartIndex + groupSize),
    }))
    const rowGroupBytes = stream.createRowGroupBytes(groupData, { pageSize })
    fs.writeFileSync(filename, rowGroupBytes, { flag: 'a' })
  }

  // Write footer
  fs.writeFileSync(filename, stream.createFooterBytes(), { flag: 'a' })
}

/**
 * Buffered file writer.
 * Writes data to a local file in chunks using node fs.
 *
 * @param {string} filename
 * @returns {Writer}
 */
export function fileWriter(filename) {
  const writer = new ByteWriter()
  const chunkSize = 1_000_000 // 1mb

  // create a new file or overwrite existing one
  fs.writeFileSync(filename, '', { flag: 'w' })

  function flush() {
    const chunk = writer.buffer.slice(0, writer.index)
    // TODO: async
    fs.writeFileSync(filename, new Uint8Array(chunk), { flag: 'a' })
    writer.index = 0
  }

  /**
   * Override the ensure method
   * @param {number} size
   */
  writer.ensure = function(size) {
    if (writer.index > chunkSize) {
      flush()
    }
    if (writer.index + size > writer.buffer.byteLength) {
      const newSize = Math.max(writer.buffer.byteLength * 2, writer.index + size)
      const newBuffer = new ArrayBuffer(newSize)
      new Uint8Array(newBuffer).set(new Uint8Array(writer.buffer))
      writer.buffer = newBuffer
      writer.view = new DataView(writer.buffer)
    }
  }
  writer.getBuffer = function() {
    throw new Error('getBuffer not supported for FileWriter')
  }
  writer.finish = function() {
    flush()
  }
  return writer
}
