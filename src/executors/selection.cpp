#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseSELECTION()
{
    logger.log("syntacticParseSELECTION");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[6] != "FROM")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SELECTION;
    parsedQuery.selectionResultRelationName = tokenizedQuery[0];
    parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
    parsedQuery.selectionRelationName = tokenizedQuery[7];

    string binaryOperator = tokenizedQuery[4];
    if (binaryOperator == "<")
        parsedQuery.selectionBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.selectionBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.selectionBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.selectionBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.selectionBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.selectionBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    regex numeric("[-]?[0-9]+");
    string secondArgument = tokenizedQuery[5];
    if (regex_match(secondArgument, numeric))
    {
        parsedQuery.selectType = INT_LITERAL;
        parsedQuery.selectionIntLiteral = stoi(secondArgument);
    }
    else
    {
        parsedQuery.selectType = COLUMN;
        parsedQuery.selectionSecondColumnName = secondArgument;
    }
    return true;
}

bool semanticParseSELECTION()
{
    logger.log("semanticParseSELECTION");

    if (tableCatalogue.isTable(parsedQuery.selectionResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.selectionRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionFirstColumnName, parsedQuery.selectionRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (parsedQuery.selectType == COLUMN)
    {
        if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionSecondColumnName, parsedQuery.selectionRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }
    return true;
}

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator)
{
    switch (binaryOperator)
    {
    case LESS_THAN:
        return (value1 < value2);
    case GREATER_THAN:
        return (value1 > value2);
    case LEQ:
        return (value1 <= value2);
    case GEQ:
        return (value1 >= value2);
    case EQUAL:
        return (value1 == value2);
    case NOT_EQUAL:
        return (value1 != value2);
    default:
        return false;
    }
}

void retrieveResult(Table *table, Table *resultantTable, int bucket)
{
    Cursor cursor = table->getCursor(bucket, 0);
    vector<int> row = cursor.getNextInBucket();
    while (!row.empty())
    {
        int value1 = row[table->getColumnIndex(parsedQuery.selectionFirstColumnName)];
        int value2 = parsedQuery.selectionIntLiteral;
        if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
            resultantTable->writeRow<int>(row);
        row = cursor.getNextInBucket();
    }
}

void saveResult(Table *resultantTable)
{
    if (resultantTable->blockify())
        tableCatalogue.insertTable(resultantTable);
    else{
        cout<<"Empty Table"<<endl;
        resultantTable->unload();
        delete resultantTable;
    }
}

void executeSELECTION()
{
    logger.log("executeSELECTION");
    Table *table = tableCatalogue.getTable(parsedQuery.selectionRelationName);
    Table *resultantTable = new Table(parsedQuery.selectionResultRelationName, table->columns);
    int firstColumnIndex = table->getColumnIndex(parsedQuery.selectionFirstColumnName);
    if (!table->indexed)
    {
        Cursor cursor = table->getCursor();
        vector<int> row = cursor.getNext();
        int secondColumnIndex;
        if (parsedQuery.selectType == COLUMN)
            secondColumnIndex = table->getColumnIndex(parsedQuery.selectionSecondColumnName);
        while (!row.empty())
        {
            int value1 = row[firstColumnIndex];
            int value2;
            if (parsedQuery.selectType == INT_LITERAL)
                value2 = parsedQuery.selectionIntLiteral;
            else
                value2 = row[secondColumnIndex];
            if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
                resultantTable->writeRow<int>(row);
            row = cursor.getNext();
        }
        saveResult(resultantTable);
        return;
    }
    else if (table->indexingStrategy == HASH)
    {
        if ((table->indexed && table->getIndexedColumn() != parsedQuery.selectionFirstColumnName) || (parsedQuery.selectType == COLUMN) || (parsedQuery.selectionBinaryOperator == NOT_EQUAL))
        {
            Cursor cursor = table->getCursor(0, 0);
            vector<int> row = cursor.getNextInAllBuckets();
            int secondColumnIndex;
            if (parsedQuery.selectType == COLUMN)
                secondColumnIndex = table->getColumnIndex(parsedQuery.selectionSecondColumnName);
            while (!row.empty())
            {
                int value1 = row[firstColumnIndex];
                int value2;
                if (parsedQuery.selectType == INT_LITERAL)
                    value2 = parsedQuery.selectionIntLiteral;
                else
                    value2 = row[secondColumnIndex];
                if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
                    resultantTable->writeRow<int>(row);
                row = cursor.getNextInAllBuckets();
            }
            saveResult(resultantTable);
        }
        else if (parsedQuery.selectionBinaryOperator == EQUAL)
        {
            retrieveResult(table, resultantTable, table->hash(parsedQuery.selectionIntLiteral));
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == LESS_THAN)
        {
            for (int i = *(table->distinctValuesInColumns[firstColumnIndex].begin()), j = 0; i < parsedQuery.selectionIntLiteral, j < table->M; i++, j++)
            {
                retrieveResult(table, resultantTable, table->hash(i));
            }
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == LEQ)
        {
            for (int i = *(table->distinctValuesInColumns[firstColumnIndex].begin()), j = 0; i <= parsedQuery.selectionIntLiteral, j < table->M; i++, j++)
            {
                retrieveResult(table, resultantTable, table->hash(i));
            }
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == GREATER_THAN)
        {
            for (int i = parsedQuery.selectionIntLiteral + 1, j = 0; i < *(table->distinctValuesInColumns[firstColumnIndex].rbegin()), j < table->M; i++, j++)
            {
                retrieveResult(table, resultantTable, table->hash(i));
            }
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == GEQ)
        {
            for (int i = parsedQuery.selectionIntLiteral + 1, j = 0; i < *(table->distinctValuesInColumns[firstColumnIndex].rbegin()), j < table->M; i++, j++)
            {
                retrieveResult(table, resultantTable, table->hash(i));
            }
            saveResult(resultantTable);
            return;
        }
    }
}