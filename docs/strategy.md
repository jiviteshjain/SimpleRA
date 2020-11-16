# SimpleRA
*Jivitesh Jain, Shanmukh Karra*
---

## The BufferManager

The original buffer manager did not support updates to pages. To add that support, we introduced the idea of deferred writes. Thus, changes being made to the pages aren't written to disk immediately, but stored on memory instead. Only when that page needs to leave the memory buffer are changes to that page written back.  

This proves to be a great improvement in terms of performance, especially in a single-threaded environment, where repeated writes to the same page are expected, for example while inserting rows in bulk, finding rows, sorting etc, and the changes are cached in memory.

**Note: This means that the pages in `/data/temp` may not be updated on time, and hence may not be an accurate representation of the state of the database.**

## Operators optimized for Phase-2

### Group by

The map containing the data was stored in main memory as per the assumption that the index directory (and hence unique values in columns) would fit in main memory.

### Alter

For as much as was possible, the index structure was preserved. For example, for linear hash, modified rows were added back to the same buckets to avoid costly re-indexing.

### Bulk insert

Whereever possible, rows of the table were directly written in chunks of pages, instead of writings individual rows.