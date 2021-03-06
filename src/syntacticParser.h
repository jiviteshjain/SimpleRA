#ifndef __SYNTACTICPARSER_H
#define __SYNTACTICPARSER_H

#include "tableCatalogue.h"
#include "matrixCatalogue.h"

using namespace std;

enum DataType
{
    TABLE,
    MATRIX
};

enum QueryType
{
    ALTERTABLE,
    BULKINSERT,
    CLEAR,
    CROSS,
    DELETE,
    DISTINCT,
    EXPORT,
    GROUPBY,
    INSERT,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    LOADMATRIX,
    PRINT,
    PROJECTION,
    RENAME,
    SELECTION,
    SORT,
    SOURCE,
    TRANSPOSE,
    UNDETERMINED
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string alterTableRelationName = "";
    string alterTableOperationName = "";
    string alterTableColumnName = "";

    string bulkInsertFileName = "";
    string bulkInsertRelationName = "";

    string clearRelationName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string deleteRelationName = "";
    vector<int> deleteRow;

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportRelationOrMatrixName = "";

    string groupByGroupingAttributeName = "";
    string groupByResultRelationName = "";
    string groupByRelationName = "";
    string groupByOperatorName = "";
    string groupByAttributeName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";

    string insertRelationName = "";
    vector<int> insertRow;

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadRelationName = "";
    string loadMatrixName = "";

    string transposeMatrixName = ""; 

    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";

    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";
    int sortBufferSize = 0;

    string sourceFileName = "";

    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseALTERTABLE();
bool syntacticParseBULKINSERT();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDELETE();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseGROUPBY();
bool syntacticParseINDEX();
bool syntacticParseINSERT();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParseLOADMATRIX();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseTRANSPOSE();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);
bool isMatrix(string matrixName);

#endif