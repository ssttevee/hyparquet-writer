import { getSchemaPath } from 'hyparquet/src/schema.js'
import { ByteWriter } from './bytewriter.js'
import { writeColumn } from './column.js'
import { encodeNestedValues, normalizeValue } from './dremel.js'
import { writeIndexes } from './indexes.js'
import { writeMetadata } from './metadata.js'
import { snappyCompress } from './snappy.js'

/**
 * ParquetStream class for streaming parquet file generation.
 * Centralizes offset/state management and emits Parquet bytes in phases.
 *
 * @import {ColumnChunk, CompressionCodec, DecodedArray, FileMetaData, KeyValue, RowGroup, SchemaElement} from 'hyparquet'
 * @import {ColumnEncoder, ColumnSource, Compressors, PageIndexes} from '../src/types.js'
 */
export class ParquetStream {
  /**
   * @param {object} options
   * @param {SchemaElement[]} options.schema
   * @param {CompressionCodec} [options.codec]
   * @param {Compressors} [options.compressors]
   * @param {boolean} [options.statistics]
   * @param {KeyValue[]} [options.kvMetadata]
   */
  constructor({ schema, codec = 'SNAPPY', compressors, statistics = true, kvMetadata }) {
    this.schema = schema
    this.codec = codec
    // Include built-in snappy as fallback
    this.compressors = { SNAPPY: snappyCompress, ...compressors }
    this.statistics = statistics
    this.kvMetadata = kvMetadata

    /** @type {RowGroup[]} */
    this.row_groups = []
    this.num_rows = 0n

    /** @type {PageIndexes[]} */
    this.pendingIndexes = []

    // Track absolute offset across all emitted chunks
    this.offset = 0
  }

  /**
   * Create header bytes (PAR1 magic).
   * @returns {Uint8Array}
   */
  createHeaderBytes() {
    const writer = new ByteWriter()
    writer.appendUint32(0x31524150) // PAR1
    this.offset = writer.offset
    return new Uint8Array(writer.getBuffer())
  }

  /**
   * Create bytes for one row group.
   * @param {ColumnSource[]} columnData
   * @param {object} [options]
   * @param {number} [options.pageSize]
   * @returns {Uint8Array}
   */
  createRowGroupBytes(columnData, options = {}) {
    const { pageSize = 1048576 } = options
    
    const writer = new ByteWriter()
    // Set the writer's offset to match our absolute offset
    writer.offset = this.offset

    const groupStartOffset = writer.offset
    /** @type {ColumnChunk[]} */
    const columns = []

    const groupSize = columnData[0]?.data?.length || 0

    // write columns
    for (let j = 0; j < columnData.length; j++) {
      const { name, data, encoding, columnIndex = false, offsetIndex = true } = columnData[j]

      const schemaTreePath = getSchemaPath(this.schema, [name])
      const leafPaths = getLeafSchemaPaths(schemaTreePath)
      const columnNode = schemaTreePath.at(-1)
      /** @type {DecodedArray} */
      const normalizedData = columnNode
        ? Array.from(data, row => normalizeValue(columnNode, row))
        : Array.from(data)

      for (const leafPath of leafPaths) {
        const schemaPath = leafPath.map(node => node.element)
        const element = schemaPath.at(-1)
        if (!element) throw new Error(`parquet column ${name} missing schema element`)

        const pageData = encodeNestedValues(schemaPath, normalizedData)
        const columnValues = pageData.values

        /** @type {ColumnEncoder} */
        const column = {
          columnName: schemaPath.length === 2 ? name : schemaPath.slice(1).map(s => s.name).join('.'),
          element,
          schemaPath,
          codec: this.codec,
          compressors: this.compressors,
          stats: this.statistics,
          pageSize,
          columnIndex,
          offsetIndex,
          encoding,
        }

        const result = writeColumn({
          writer,
          column,
          values: columnValues,
          pageData,
        })

        columns.push(result.chunk)
        this.pendingIndexes.push(result)
      }
    }

    this.num_rows += BigInt(groupSize)
    this.row_groups.push({
      columns,
      total_byte_size: BigInt(writer.offset - groupStartOffset),
      num_rows: BigInt(groupSize),
    })

    // Update our absolute offset
    this.offset = writer.offset

    return new Uint8Array(writer.getBuffer())
  }

  /**
   * Create footer bytes (indexes, metadata, trailing PAR1).
   * @returns {Uint8Array}
   */
  createFooterBytes() {
    const writer = new ByteWriter()
    // Set the writer's offset to match our absolute offset
    writer.offset = this.offset

    // Write all indexes at end of file
    writeIndexes(writer, this.pendingIndexes)

    // write metadata
    /** @type {FileMetaData} */
    const metadata = {
      version: 2,
      created_by: 'hyparquet',
      schema: this.schema,
      num_rows: this.num_rows,
      row_groups: this.row_groups,
      metadata_length: 0,
      key_value_metadata: this.kvMetadata,
    }
    // @ts-ignore don't want to actually serialize metadata_length
    delete metadata.metadata_length
    writeMetadata(writer, metadata)

    // write footer PAR1
    writer.appendUint32(0x31524150)

    // Update our absolute offset
    this.offset = writer.offset

    return new Uint8Array(writer.getBuffer())
  }
}

/**
 * Expand a schema path to all primitive leaf nodes under the column.
 *
 * @import {SchemaTree} from 'hyparquet/src/types.js'
 * @param {SchemaTree[]} schemaPath
 * @returns {SchemaTree[][]}
 */
function getLeafSchemaPaths(schemaPath) {
  /** @type {SchemaTree[][]} */
  const leaves = []
  dfs([...schemaPath])
  return leaves

  /**
   * @param {SchemaTree[]} path
   */
  function dfs(path) {
    const node = path[path.length - 1]
    if (!node.children.length) {
      leaves.push(path)
      return
    }
    for (const child of node.children) {
      dfs([...path, child])
    }
  }
}
