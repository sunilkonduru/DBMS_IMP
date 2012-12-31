
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>
#include<cmath>
#include "../pf/pf.h"
#include <iostream>
using namespace std;

#define slotControlBuffers 2 // one for storing next-free-space offset and other is length of slots directory
// Return code
typedef int RC;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;

// Slots entry in directory list of each page <offset><length>
typedef struct
{
	int offset;
	int length;
} SlotEntry;

typedef struct
{
	vector<SlotEntry> list;
	int freeSpaceOffset;
	int length;
} SlotsDirectory;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef enum { NaturalRecord = 0 , MutatedRecord , ReferencedRecord} DataRecordType;
// Natural record 0 refers to data in the current slot
// Mutated record 1 contains the rid of the new location
// ReferencedRecord refers 2 to a record with data referenced from a mutated record .
// Note : Referenced record is not a part of scan as it is not a natural record

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};


// Comparison Operator
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
  RM_ScanIterator();
  ~RM_ScanIterator();

  // "data" follows the same format as RM::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  RC readAttributeType(const string tableName, const string attributeName,Attribute &attr);
  vector<RID> ridList;
  vector<string> attrNames;
  int currentIndex;
  string tableName;
  vector<Attribute> _attrs;
};


// Record Manager
class RM
{
public:
  static RM* Instance();

  RC createTable(const string tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string tableName);

  RC getAttributes(const string tableName, vector<Attribute> &attrs);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
  RC insertTuple(const string tableName, const void *data, RID &rid);

  RC deleteTuples(const string tableName);

  RC deleteTuple(const string tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string tableName, const void *data, const RID &rid);

  RC readTuple(const string tableName, const RID &rid, void *data);

  RC readAttribute(const string tableName, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(const string tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


public:
  RC initialSetup();
  RC createCatalog();
  RC createAttribute();
  PF_Manager *_pf;
  PF_FileHandle fileHandle;
  RC headerPage(const void* data);
  RC dataPage(const void* data);
  RC getDirectoryList(const void *data,SlotsDirectory &directory);
  RC insertDirectoryList(const void* data,SlotsDirectory directory);
  RC prepareCatalogTuple(int table_id,string tablename,float version,const void *data,int *tupleSize);
  RC prepareAttributeTuple(int table_id,string columnName,int columnType,int length,void *data,int *tupleSize);
  RC writeToBuffer(const void* buffer,const void* data, int offset1,int offset2,int length);
  RC tableStructure(int type,vector<Attribute> &attrs);
  RC printDirectory(SlotsDirectory directory);
  RC freeSpaceInTable(const string tableName,int tupleLength,int *page,int *offset);
  RC freeSpaceInPage(void *page,int tupleLength,int *offset);
  RC getAllPagesInHeader(const string tableName,vector<int> &pages);
  RC findTableId(const string tableName,int *table_id,RID &rid);
  RC findAttributesforTable(int table_id,vector<Attribute> &attrs);
  RC addPageToHeader(PF_FileHandle &tableHandle);
  RC readAttributeType(const string tableName, const string attributeName,Attribute &attr);
  RC getRecordLength(const string tableName,int *recordLength,const void *data);
// Extra credit
public:
  RC dropAttribute(const string tableName, const string attributeName);

  RC addAttribute(const string tableName, const Attribute attr);

  RC reorganizeTable(const string tableName);



protected:
  RM();
  ~RM();

private:
  static RM *_rm;
};

#endif
