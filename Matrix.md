# Matrix implementation details

## Idea

Our approach is splitting the input **NxN** matrix into smaller **MxM** matrices, such that each MxM matrix can fit in a block. We know that the **block size** is 8 kilobytes. Therefore, the value of M can be calculated as `(int)sqrt((BLOCK_SIZE * 1024) / sizeof(int))`, which is 45. Therefore, the input NxN matrix is split into 45x45 matrices, where each 45x45 matrix is then stored in a page.

The main reason for opting for this storage strategy was to be able to transpose efficiently. Storing row-wise spanning across pages wouldn't have been effective for transpose, as it would have required a lot more page accesses.

## Code implementation

In order to implement the LOAD MATRIX and TRANSPOSE commands (and modify EXPORT), the following changes were made to the code:

 - The **matrix** class was created, which resembles the table class in terms of functionality
 -  The **matrixCatalogue** class was created, which resembles the tableCatalogue class in terms of functionality
 - The **Page** class and **BufferManager** classes were modified, adding functions such as getMatrix(), writeMatrixPage(), getMatrixPage(), and deleteFile()
 -  The **syntacticParser**, **semanticParser**, and **executor** classes were modified to support the new commands
 - Two new executors were created (**loadMatrix** and **transpose**), and the **export** executor was modified
 - **FileTypeCheck** was created to handle errors when trying to load matrices incorrectly or transpose relations

## LOAD MATRIX

The LOAD MATRIX command calls the **loadMatrix** executor, which then instantiates the Matrix class, invoking its **constructor**. After the instantiation, **matrix.load()** is called, which calls **matrix.blockify()**. The **blockify()** function is responsible for reading the matrix, splitting it into smaller 45*45 matrices, and writing these matrices to pages by calling **bufferManager.writeMatrixPage()**, which then calls **page.writeMatrixPage()**.

The **blockify()** function works as follows:

 - Call **initCalc(firstLineOfMatrix)**, which calculates the dimensions of the input matrix and determines the number of blocks required, calculated as `blockCount = ceil((double)dimension/(double)45)`
 - Initialize input stream for the source file (input matrix)
 - Loop **block_j** from 0 to blockCount. In each iteration, initialize **block_i** to 0, read through all the rows of the input matrix, storing the 45 integers starting from the **block_j*45** position in each row (for example, in the first iteration, read from 0th column to 44th column. In the second iteration, read from 45th column to 89th column, and so on). While reading through all the rows, **stop after every 45 rows**, store the 45x45 matrix read so far in a vector<vector< int>>, write the 45x45 matrix to a page indexed as **page(block_i, block_j)**, and increment block_i
 -  **Note**: As the NxN matrix may not perfectly fit into 45x45 matrices, the remaining space in 45x45 matrices is padded with -1s, as the input matrix only contains positive integers

## TRANSPOSE

The TRANSPOSE command calls the **transpose** executor, which finds a matrix of the given name from matrixCatalogue, and calls the matrix.transpose() function.

The **transpose()** function works as follows:

 - Loop **rowIndex** from 0 to blockCount, loop **colIndex** from i+1 to blockCount (**nested** loops). In each inner iteration:
	 -  Get the **(rowIndex, colIndex)** page and **(colIndex, rowIndex)** page
	 - Retrieve the matrices from each of these pages
	 - **Transpose each matrix internally**
	 - **Swap these matrices within their pages**, i.e, write the second matrix into the first page and the first page into the second matrix
 - Loop rowIndex from 0 to blockCount. In each iteration: (**for diagonal matrices)**
	 - Get the (rowIndex, rowIndex) page
	 - Retrieve the matrix from this page
	 - Transpose the matrix internally
	 - Write the matrix back to the page 

## EXPORT MATRIX

After the `EXPORT` command makes it through the parsers and executors, the `Matrix` object's `makePermanent` method is invoked, which is responsible for writing the matrix into its CSV file.

We decided against using the `Cursor` class for reading lines from pages because of the design of our pages: they aren't arranged row-wise.

The algorithm for exporting works as follows:
- We write the CSV file row-wise. For every row of the file:
     - Calculate which row of pages would that row fall into as **row_number / 45**.
     - Calculate which row in those pages corresponds to this row in the matrix as **row_number MODULO 45**.
     - Iterate over all the pages in that row, copying their corresponding rows into a `vector<int>`, building this row of the matrix.
     - Write this row to a file.

*As an optimisation, instead of reading one row at a time, we read `ROWS_AT_ONCE` rows at a time to speed things up. The above description stays largely the same, the only changes being that it is a `vector<vector<int>>` now, and some conditions for handling loop edges need to be added. Depending on the available RAM size, this can be set to a small value such as 2 or 5.

## NUMBER OF BLOCK ACCESSES

Define `BLOCK_DIM = floor(sqrt((BLOCK_SIZE * 1024) / sizeof(int)))`
Define `NUM_BLOCKS = N*N / (BLOCK_DIM * BLOCK_DIM)`

**Load:** Each block is accessed only once, so `NUM_BLOCKS` accesses. The CSV file is read `N/BLOCK_DIM` times.

**TRANSPOSE:** Each block is accessed only once for read and write each, so `2 * NUM_BLOCKS` accesses.

**EXPORT:** Each block is accessed `BLOCK_DIM / ROWS_AT_ONCE` times. `ROWS_AT_ONCE` is a small constant, so this means roughly `NUM_BLOCKS * BLOCK_DIM` accesses, and precisely `NUM_BLOCKS * BLOCK_DIM / ROWS_AT_ONCE` accesses.