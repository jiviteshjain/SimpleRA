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
    }



}

void Matrix::initCalc(const string& line) {
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