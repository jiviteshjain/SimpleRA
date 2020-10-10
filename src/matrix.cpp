#include "global.h"
#include "matrix.h"

Matrix::Matrix() {
    logger.log("Matrix::Matrix");
}

Matrix::Matrix(const string& matrixName) {
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

bool Matrix::load() {
    // written just to keep things consistent
    logger.log("Matrix::load");
    return this->blockify();
}

vector<int> Matrix::readLine(const string& line, int colNo) {
    
    stringstream s(line);
    int j = 0;
    string word;
    
    for (; j < colNo * MATRIX_PAGE_DIM; j++) {
        if (!getline(s, word, ',')) {
            return vector<int> (0);
        }
    }

    vector<int> out;

    for (; j < min(this->dimension, (long long int)(colNo + 1) * MATRIX_PAGE_DIM); j++) {
        if (!getline(s, word, ',')) {
            return vector<int> (0);
        }
        word = trim(word);
        out.push_back(stoi(word));
    }

    while(out.size() < MATRIX_PAGE_DIM) {
        out.push_back(-1);
    }

    return out;

}

bool Matrix::blockify() {
    logger.log("Matrix::blockify");
    
    // make calculations
    ifstream fin(this->sourceFileName, ios::in);

    if (fin.peek() == ifstream::traits_type::eof()) {
        cout << "OPERATIONAL ERROR: File is empty." << endl;
        return false;
    }

    string firstline;
    getline(fin, firstline);
    this->initCalc(firstline);

    fin.close();

    // store into blocks, column by column
    for (int block_j = 0; block_j < this->blockCount; block_j++) {
        ifstream fin(this->sourceFileName, ios::in);

        int block_i = 0;
        int in_this_block = 0;
        vector<vector<int>> this_page (MATRIX_PAGE_DIM, vector<int> (MATRIX_PAGE_DIM, -1));

        string line;
        while (getline(fin, line)) {
            if (in_this_block == MATRIX_PAGE_DIM) {

                bufferManager.writeMatrixPage(this->matrixName, block_i, block_j, this_page);

                // clear the vector
                this_page.assign(MATRIX_PAGE_DIM, vector<int> (MATRIX_PAGE_DIM, -1));
                block_i++;
                in_this_block = 0;
            }

            this_page[in_this_block] = this->readLine(line, block_j);
            in_this_block++;
        }

        // rest of the last page is already -1
        bufferManager.writeMatrixPage(this->matrixName, block_i, block_j, this_page);


        fin.close();
        return true;
    }



}

void Matrix::initCalc(const string& line) {
    logger.log("Matrix::initCalc");
    
    int count = 0;
    for (auto& c: line) {
        if (c == ',') {
            count++;
        }
    }
    if (line.back() != ',') {
        count++;
    }
    this->dimension = count;
    this->blockCount = ceil((double)this->dimension/(double)MATRIX_PAGE_DIM);
}

void Matrix::makePermanent() {
    logger.log("Matrix::makePermanent");

    ofstream fout(this->sourceFileName, ios::trunc);

    vector<vector<int>> this_page; // avoid multiple constructor and destructor calls
    for (long long int i = 0; i < this->dimension;) {

        long long int j = 0;
        // i and j are indices in large matrix
        
        int block_i = i / MATRIX_PAGE_DIM;
        int line_in_block = i % MATRIX_PAGE_DIM;

        int can_go_upto = min((long long int)ROWS_AT_ONCE, min((long long int)(this->dimension - i), (long long int)(MATRIX_PAGE_DIM - line_in_block)));

        vector<vector<int>> lines(can_go_upto);
        for (auto& line : lines) {
            line.reserve(this->dimension);
        }

        int block_j = 0;
        for (; block_j < this->blockCount - 1; block_j++) {
            this_page = bufferManager.getMatrixPage(this->matrixName, block_i, block_j).getMatrix();
            
            for (int k = 0; k < can_go_upto; k++) {
                lines[k].insert(lines[k].end(), this_page[line_in_block + k].begin(), this_page[line_in_block + k].end());
            }

            j = j + MATRIX_PAGE_DIM;
        }

        this_page = bufferManager.getMatrixPage(this->matrixName, block_i, block_j).getMatrix();
        for (int k = 0; k < can_go_upto; k++) {
            lines[k].insert(lines[k].end(), this_page[line_in_block + k].begin(), this_page[line_in_block + k].begin() + (this->dimension - j));
        }
        this->writeLine(lines, fout);
        i = i + can_go_upto;
    }

    fout.close();
}

void Matrix::writeLine(const vector<vector<int>>& lines, ofstream& fout) {
    logger.log("Matrix::writeLine");

    for (auto &line : lines) {
        for (long long int j = 0; j < line.size() - 1; j++) {
            fout << line[j] << ",";
        }
        fout << line.back() << endl;
    }
    
}
void Matrix::transpose()
{
    for (int rowIndex = 0; rowIndex < this->blockCount; rowIndex++)
    {
        for (int colIndex = rowIndex + 1; colIndex < this->blockCount; colIndex++)
        {
            MatrixPage currentPage = bufferManager.getMatrixPage(this->matrixName, rowIndex, colIndex);
            vector<vector<int>> currentMatrix = currentPage.getMatrix();

            MatrixPage swapPage = bufferManager.getMatrixPage(this->matrixName, colIndex, rowIndex);
            vector<vector<int>> swapMatrix = swapPage.getMatrix();

            // transposing inside the matrices
            for (int i = 0; i < MATRIX_PAGE_DIM; i++)
                for (int j = i + 1; j < MATRIX_PAGE_DIM; j++)
                {
                    swap(currentMatrix[i][j], currentMatrix[j][i]);
                    swap(swapMatrix[i][j], swapMatrix[j][i]);
                }

            bufferManager.writeMatrixPage(this->matrixName, rowIndex, colIndex, swapMatrix);
            bufferManager.writeMatrixPage(this->matrixName, colIndex, rowIndex, currentMatrix);
        }
    }

    // for blocks on diagonal
    for (int rowIndex = 0; rowIndex < this->blockCount; rowIndex++)
    {
        MatrixPage currentPage = bufferManager.getMatrixPage(this->matrixName, rowIndex, rowIndex);
        vector<vector<int>> currentMatrix = currentPage.getMatrix();
        
        // transposing inside the matrix
        for (int i = 0; i < MATRIX_PAGE_DIM; i++)
            for (int j = i + 1; j < MATRIX_PAGE_DIM; j++)
                swap(currentMatrix[i][j], currentMatrix[j][i]);
        
        bufferManager.writeMatrixPage(this->matrixName, rowIndex, rowIndex, currentMatrix);
    }
}

void Matrix::unload() {
    for (long long int i = 0; i < this->blockCount; i++) {
        for (long long int j = 0; j < this->blockCount; j++) {
            bufferManager.deleteMatrixFile(this->matrixName, i, j);
        }
    }
}