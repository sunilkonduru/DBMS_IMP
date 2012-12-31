
#ifndef _ix_h_
#define _ix_h_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "../pf/pf.h"
#include "../rm/rm.h"

# define IX_EOF (-1)  // end of the index scan
#include <iostream>
using namespace std;

// Header Information of the index Page
typedef int NodePointer;

typedef enum {LeafNodeType = 0 , NonleafNodeType,RootNodeType} NodeType;

typedef struct
{
	NodePointer rootPage;
	unsigned orderOfBTree;
	AttrType attrType;
}HeaderInfo;

typedef struct
{
	int keyInt;
	float keyFloat;
	string keyVarChar;
}LeafValue;

typedef struct
{
	RID rid;
//	int key;
	LeafValue keyValue;

}LeafEntry;



typedef struct
{
	vector<LeafEntry> keys;
	NodePointer next_page;
	NodePointer prev_page;
	int numberOfEntries;
	NodeType type;
	int extrabit;

}LeafNode;

typedef struct
{
	vector<LeafValue> keys;
	vector<NodePointer> pointers;
	int extraBit1;
	int extraBit2;
	int numberOfKeys;
	NodeType type;
	int extrabit;

}NonLeafNode;


class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
  
 public:
  PF_Manager *_pf;
  string IndexFileName(const string tableName,       // string for tableName & attribute name
			 const string attributeName);


 public:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor
 
 public:
  static IX_Manager *_ix_manager;
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  ();                           // Constructor
  ~IX_IndexHandle ();                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry
  RC setIndexFileName(string indexFileName);

 public:
  RC GetHeaderInfo(HeaderInfo &info);
  RC SetHeaderInfo(HeaderInfo &info);
  RC GetLeafNode(const NodePointer pagenum,LeafNode &node);
  RC SetLeafNode(const NodePointer pagenum,LeafNode &node);
  RC GetNonLeafNode(const NodePointer pagenum,NonLeafNode &node);
  RC SetNonLeafNode(const NodePointer pagenum,NonLeafNode &node);
  RC writeToBuffer(const void* buffer,const void* data, int offset1,int offset2,int length);
  RC getNodeType(const int pageNum,NodeType &type);
  RC FindIndexInNode(NodePointer nodepointer,int &index,LeafEntry entry);

  RC InsertEntry(NodePointer nodePointer,LeafEntry entry,NodePointer &newChildEntry);
  RC locateNewLeafElement(LeafNode leafNode,LeafValue key,int &value);
  RC FindPointerInNode(NodePointer nodepointer,int &index,LeafEntry entry);


  RC Delete(NodePointer parentpointer, NodePointer nodepointer, LeafEntry entry, NodePointer &oldchildEntry);
  RC FindSiblingsForParent(NodePointer parent,NodePointer nodepointer,NodePointer &leftSibling,NodePointer &rightSibling,int &tempOldChildPointer);

  RC find(LeafValue key,NodePointer &leafnode);
  RC tree_search(NodePointer node,LeafValue key,NodePointer &leafnode);

  RC printLeafNode(LeafNode node);
  RC printNonLeafNode(NonLeafNode node);

  RC GetLeafValue(void *key,LeafValue &value);

 public:
  string _indexFileName;
  string _tableName;
  string _attributeName;
  AttrType _type;
  PF_Manager *_pf;
  HeaderInfo _headerInfo;
  int orderOfBTree;

};


class IX_IndexScan {
 public:
  IX_IndexScan();  								// Constructor
  ~IX_IndexScan(); 								// Destructor


  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);           

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan

 public:
 	NodePointer nextVisitedNode;
 	IX_IndexHandle _indexHandle;
 	CompOp _compOp;
 	int _valueInt;
 	float _valueFloat;
 	string _valueVarChar;
 	AttrType _attrType ;
 	bool _isScanOpen;
// 	int _nextIndex;
 	vector<RID> rids;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
