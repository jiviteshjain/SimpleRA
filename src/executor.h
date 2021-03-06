#ifndef __EXECUTOR_H
#define __EXECUTOR_H

#include"semanticParser.h"

void executeCommand();

void executeALTERTABLE();
void executeBULKINSERT();
void executeCLEAR();
void executeCROSS();
void executeDELETE();
void executeDISTINCT();
void executeEXPORT();
void executeGROUPBY();
void executeINSERT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executeLOADMATRIX();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeTRANSPOSE();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);

#endif