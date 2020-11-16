//Server Code
#include "global.h"

using namespace std;

float BLOCK_SIZE = 8;
uint BLOCK_COUNT = 10;
uint PRINT_COUNT = 20;
Logger logger;
vector<string> tokenizedQuery;
ParsedQuery parsedQuery;
BufferManager bufferManager; // Required for deleting TableCatalogue and MatrixCatalogue objects
TableCatalogue tableCatalogue;
MatrixCatalogue matrixCatalogue;
int MATRIX_PAGE_DIM;

void doCommand()
{
    logger.log("doCommand");
    if (syntacticParse() && semanticParse())
        executeCommand();
    return;
}

int main(void)
{

    regex delim("[^\\s,]+");
    string command;
    system("rm -rf ../data/temp");
    system("mkdir ../data/temp");
    
    MATRIX_PAGE_DIM = (int)sqrt((BLOCK_SIZE * 1024) / sizeof(int));
    
    while(!cin.eof())
    {
        cout << "\n> ";
        tokenizedQuery.clear();
        parsedQuery.clear();
        logger.log("\nReading New Command: ");
        getline(cin, command);
        logger.log(command);


        auto words_begin = std::sregex_iterator(command.begin(), command.end(), delim);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
            tokenizedQuery.emplace_back((*i).str());

        if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
        {
            break;
        }

        if (tokenizedQuery.empty())
        {
            continue;
        }

        if (tokenizedQuery.size() == 1)
        {
            cout << "SYNTAX ERROR" << endl;
            continue;
        }

        doCommand();
    }
}