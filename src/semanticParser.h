#ifndef __SEMANTICPARSER_H
#define __SEMANTICPARSER_H

#include"syntacticParser.h"

bool semanticParse();

bool semanticParseBULKINSERT();
bool semanticParseCLEAR();
bool semanticParseCROSS();
bool semanticParseDELETE();
bool semanticParseDISTINCT();
bool semanticParseEXPORT();
bool semanticParseGROUPBY();
bool semanticParseINDEX();
bool semanticParseINSERT();
bool semanticParseJOIN();
bool semanticParseLIST();
bool semanticParseLOAD();
bool semanticParseLOADMATRIX();
bool semanticParsePRINT();
bool semanticParsePROJECTION();
bool semanticParseRENAME();
bool semanticParseSELECTION();
bool semanticParseSORT();
bool semanticParseSOURCE();
bool semanticParseTRANSPOSE();

#endif