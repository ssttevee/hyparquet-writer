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
export function groupIterator({ columnDataRows, rowGroupSize }) {
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
