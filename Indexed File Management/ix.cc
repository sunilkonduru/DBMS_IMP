
#include "ix.h"
#include <vector>
#include <string>
#include "stdlib.h"
#include "string.h"
#include <algorithm>
#include "algorithm"

const int ix_success = 0;
const int ix_failure = -1;
const int ix_endoffile = -1;
const int ix_filenotfound = -3;

//const unsigned int orderOfBTree =50;
const int headerPageOffset = 0;
const int size_int = sizeof(int);
const int size_float = sizeof(float);
const int size_nodepointer = sizeof(NodePointer);

const int startOfPage = 0;
const int endOfPage = PF_PAGE_SIZE;
const NodePointer NullPage = -1;
const int EndOfFile= -2;


IX_Manager* IX_Manager::_ix_manager = 0;

//*************** Index Manager ***************
  IX_Manager* IX_Manager::Instance()
  {
	  if(!_ix_manager)
	  {
		   _ix_manager = new IX_Manager();
	  }
	  return _ix_manager;
  }

  RC IX_Manager::CreateIndex(const string tableName,       // create new index
		 const string attributeName)
  {
	  string indexFile = IndexFileName(tableName,attributeName);
	  RC rc = _pf->fileExists(indexFile.c_str());
	  if(!rc)
	  {
		  int rootPageNum = 1 , LeafPageNum = 2 ; // since header is at page num 0
		  // 1. Create File and add 3 pages to the new file
		  _pf->CreateFile(indexFile.c_str());
		  RM *rm= RM::Instance();
		  Attribute attr;
		  rm->readAttributeType(tableName,attributeName,attr);
		  PF_FileHandle fileIndexHandle;
		  char page[PF_PAGE_SIZE];
		  _pf->OpenFile(indexFile.c_str(),fileIndexHandle);
		  fileIndexHandle.AppendPage(&page); // header page
		  fileIndexHandle.AppendPage(&page); // root page
		  fileIndexHandle.AppendPage(&page); // leaf page
		  _pf->CloseFile(fileIndexHandle);

		  // calculate the order of btree
		  int _orderOfBTree = 50; // dummy value
		  const int sizeOfRID = sizeof(RID);
		  if(attr.type==TypeInt)
		  {
			  _orderOfBTree = (int)((PF_PAGE_SIZE*0.4)/(size_int+sizeOfRID)); // <key 4 bytes><rid pair 8 bytes> 12 bytes
		  }
		  else if(attr.type==TypeReal)
		  {
			  _orderOfBTree = (int)((PF_PAGE_SIZE*0.4)/(size_float+sizeOfRID)); // <key 4 bytes><rid pair 8 bytes> 12 bytes
		  }
		  else if(attr.type == TypeVarChar)
		  {
			  _orderOfBTree = (int)((PF_PAGE_SIZE*0.4)/(attr.length+size_int+sizeOfRID)); // <key 4 bytes length + string length><rid pair 8 bytes> 12 bytes
		  }
		  else
		  {

		  }

		  // 2. Add Header Info of index file
		  HeaderInfo info;
		  info.attrType = attr.type;
		  info.orderOfBTree = _orderOfBTree;
		  info.rootPage = 1;
		  IX_IndexHandle indexHandle;
		  OpenIndex(tableName,attributeName,indexHandle);
		  indexHandle.SetHeaderInfo(info);

		  // 3. Add the root node to the Index
		  NonLeafNode nonleafnode;
		  nonleafnode.extraBit1 = 0 , nonleafnode.extraBit2 = 0 , nonleafnode.extrabit = 0 ;
		  nonleafnode.numberOfKeys = 0;
		  nonleafnode.type = RootNodeType;
		  nonleafnode.pointers.push_back(LeafPageNum);
		  indexHandle.SetNonLeafNode(rootPageNum,nonleafnode);

		  // 4. Add the first leaf page
		  LeafNode leafnode;
		  leafnode.extrabit = 0 , leafnode.next_page = NullPage , leafnode.prev_page = NullPage ;
		  leafnode.numberOfEntries = 0;
		  leafnode.type = LeafNodeType;
		  indexHandle.SetLeafNode(LeafPageNum,leafnode);

		  CloseIndex(indexHandle);
		  return ix_success;
	  }
	  else
	  {
		  return ix_filenotfound;
	  }
  }

  RC IX_Manager::DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName)
  {
	  string indexName=IndexFileName(tableName,attributeName);
	  if(_pf->fileExists(indexName.c_str()))
	  {
		  _pf->DestroyFile(indexName.c_str());
		  return ix_success;
	  }
	  else
	  {
		  return ix_filenotfound;
	  }

  }

  RC IX_Manager::OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle)
  {
	  string indexName = IndexFileName(tableName,attributeName);
	  if(_pf->fileExists(indexName.c_str()))
	  {
		  indexHandle.setIndexFileName(indexName);
		  indexHandle._tableName = tableName;
		  indexHandle._attributeName = attributeName;
		  HeaderInfo info;
		  indexHandle.GetHeaderInfo(info);
		  indexHandle._type = info.attrType;
		  indexHandle._headerInfo = info;
		  indexHandle.orderOfBTree = info.orderOfBTree;
		  return ix_success;
	  }
	  else
	  {
		  return ix_filenotfound;
	  }

  }

  RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
  {
	  return ix_success;
  }


  //********* private functions

  string IX_Manager::IndexFileName(const string tableName,       // string for tableName & attribute name
			 const string attributeName)
  {
	  //  Index File name structure <tableName_attributeName>
	  string indexFile;
	  indexFile.append(tableName);
	  indexFile.append("_");
	  indexFile.append(attributeName);
	  return indexFile;

  }

  IX_Manager::IX_Manager   (){}                             // Constructor
  IX_Manager::~IX_Manager  (){}

  //***************** Index Handle

  IX_IndexHandle::IX_IndexHandle  (){

  }                           // Constructor
  IX_IndexHandle::~IX_IndexHandle (){}                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)  // Insert new index entry
    {
  	  //should change the headerInfo if my root is changed
  	  LeafValue value;
  	  GetLeafValue(key,value);
  	  LeafEntry entry;
  	  entry.keyValue = value;
  	  entry.rid=rid;
  	  NodePointer np=NullPage;
  	  InsertEntry(_headerInfo.rootPage,entry,np);
  	  return ix_success;
    }
    RC IX_IndexHandle::InsertEntry(NodePointer nodePointer,LeafEntry entry,NodePointer &newChildEntry)
    {
  	  NodeType type;
  	  //using index to fetch the next node pointer
  	  //looks like a mistake index is the offsetOfThePage right?
  	  int index=0;
  	  int keyIndex;
  	  vector<LeafEntry> leafEntries;
      getNodeType(nodePointer,type);

      if(type==NonleafNodeType|| type==RootNodeType)
  	  {
  		  keyIndex= FindPointerInNode(nodePointer,index,entry);
  		  InsertEntry(index,entry,newChildEntry);
  		  //left non leaf node
  		  NonLeafNode lnNode;

  		  if(newChildEntry!=NullPage)
  		  {
  		  	GetNonLeafNode(nodePointer,lnNode);
  			int totalEntries=lnNode.numberOfKeys;
  			//there was a split in leafNode or belowNode and I dont have extra space in my internal node also
  			if(totalEntries>=2*orderOfBTree)
  			{

  				NodeType nType;
  			    getNodeType(newChildEntry,nType);
  			    LeafNode lNode;
  				NonLeafNode nNode;
  				LeafValue newKey;
  				int newPointer;
  				NonLeafNode rNode;
  				int oldChildEntry;
  				//check for the children elements
  				if(nType==LeafNodeType)
  				{
  					GetLeafNode(newChildEntry,lNode);
  					newKey=lNode.keys[0].keyValue;
  					newPointer=newChildEntry;
  				}
  				else if((nType==NonleafNodeType) || (nType==RootNodeType))
  				{
  					GetNonLeafNode(newChildEntry,nNode);
  					newKey=nNode.keys[0];
  					newPointer=newChildEntry;
  					//delete the first element
  					nNode.keys.erase(nNode.keys.begin());
  					//erase the pointer also
  					nNode.pointers.erase(nNode.pointers.begin());
  					//decrease the count
  					nNode.numberOfKeys-=1;
  					SetNonLeafNode(newChildEntry,nNode);
  				}
  				//now I am in the Root or non leaf node
  				NonLeafNode currNode;
  				NonLeafNode newRNode;
  				GetNonLeafNode(nodePointer,currNode);
  				if(currNode.type==NonleafNodeType||currNode.type==RootNodeType)
  				{
  				//send the new
  			    // as I dont have space split it into two halves and set the newChildEntry value here, have already deleted the right child Node's first element
  				int splitNodeLength=orderOfBTree;
  				newRNode.numberOfKeys=0;
  				newRNode.type=NonleafNodeType;
  				int newKeyIndex1=keyIndex+1;
  				if(newKeyIndex1==currNode.keys.size())
  				{
  					currNode.keys.push_back(newKey);
  					currNode.pointers.push_back(newPointer);

  				}
  				else
  				{
  					currNode.keys.insert(currNode.keys.begin()+newKeyIndex1,newKey);
  					currNode.pointers.insert(currNode.pointers.begin()+(newKeyIndex1+1),newPointer);
  				}
  				int totalEntries=currNode.keys.size();
  				for(int indexOfNode=splitNodeLength;indexOfNode<totalEntries;indexOfNode++)
  				{

                      newRNode.keys.push_back(currNode.keys[indexOfNode]);
                      //d1 d2 d3 d4 d5 d6..first 3 elements shoudl be there in the first node and last four including the last of the first node will be present in the second node
  					newRNode.pointers.push_back(currNode.pointers[indexOfNode]);
  					newRNode.numberOfKeys+=1;

  				}
  				//have pushed d pointers need to push d+1th pointer
  				newRNode.pointers.push_back(currNode.pointers[totalEntries]);
  			    //delete all the extra keys and pointers of the current node
//  				int newKeyIndex=keyIndex+1;
  				int newKeyIndex = splitNodeLength;
  				currNode.keys.erase(currNode.keys.begin()+newKeyIndex,currNode.keys.end());
  				//while deleting leave the first element pointer of the nextNode in the currentNode
  				currNode.pointers.erase(currNode.pointers.begin()+(newKeyIndex+1),currNode.pointers.end());

  				PF_FileHandle indexHandle;
  				_pf->OpenFile(_indexFileName.c_str(),indexHandle);
  				char page[PF_PAGE_SIZE];
  				indexHandle.AppendPage(&page);
  				int indexOfNewPage=indexHandle.GetNumberOfPages()-1; //as index starts from zero
  				_pf->CloseFile(indexHandle);
  				//set both the current node and rNode
  				oldChildEntry=nodePointer;
  				SetNonLeafNode(oldChildEntry,currNode);
  				newChildEntry=indexOfNewPage;
  			    SetNonLeafNode(newChildEntry,newRNode);
  				}
  			   int newLeftPointer;
  			   int newRightPointer;
  			   NonLeafNode node;
  			   //if my child node is a root and
  			   if(currNode.type==RootNodeType)
  			   {
  				GetNonLeafNode(newChildEntry,node);
  				newKey=node.keys[0];
  				//page number of current Node
  			    newLeftPointer=oldChildEntry;
  			    //page number of rNode
  				newRightPointer=newChildEntry;
  				//delete the first element
  				node.keys.erase(node.keys.begin()+0);
  				node.pointers.erase(node.pointers.begin()+0);
  				//as I have deleted the element
  				node.numberOfKeys-=1;
  				//create a new node with element appended to it and  add left link and right link to the pointers
  				NonLeafNode newRootNode;
  				newRootNode.keys.push_back(newKey);
  				newRootNode.pointers.push_back(newLeftPointer);
  				newRootNode.pointers.push_back(newRightPointer);
  				newRootNode.type=RootNodeType;
  				newRootNode.numberOfKeys=1;
  				PF_FileHandle indexHandle;
  				_pf->OpenFile(_indexFileName.c_str(),indexHandle);
  				char page[PF_PAGE_SIZE];
  				indexHandle.AppendPage(&page);
  				int indexOfNewPage=indexHandle.GetNumberOfPages()-1;
  				_pf->CloseFile(indexHandle);
  				free(page);
  				//set current node and previous nodes to be non leaf nodes
  				currNode.type=NonleafNodeType;
  				node.type=NonleafNodeType;
  				SetNonLeafNode(oldChildEntry,currNode);
  				SetNonLeafNode(newChildEntry,node);
  				newChildEntry=NullPage;
  				//check whether expression works here or not
  				SetNonLeafNode(indexOfNewPage,newRootNode);
  				_headerInfo.rootPage=indexOfNewPage;
  				SetHeaderInfo(_headerInfo);
  				return ix_success;
  			   }
                }
  			//there was a split in the leaf node or non leaf node, but I have enough space in my current internal node
  			else
  			{
  				NodeType nType;
  				getNodeType(newChildEntry,nType);

  				LeafNode lNode;
  				NonLeafNode nNode;
  				LeafValue newKey;
  				int newPointer;

  				//check if children are leafNode or nonleafnode
                  if(nType==LeafNodeType)
                  {
                  	//here lNode is the new split node of my child
                  	//change from index to newChildEntry
                  	GetLeafNode(newChildEntry,lNode);
                  	newKey=lNode.keys[0].keyValue;
                  	newPointer=newChildEntry;

                  }
                  else if(nType==NonleafNodeType||nType==RootNodeType)
                  {
                  	//what if my children are non leaf node and are making a split in the non leaf child node and I am having space in the parent node
                  	GetNonLeafNode(newChildEntry,nNode);
                  	newKey=nNode.keys[0];
                  	newPointer=newChildEntry;
                  	//delete the first element
                  	nNode.keys.erase(nNode.keys.begin());
                  	nNode.pointers.erase(nNode.pointers.begin());
                  	//as I have deleted the element
                  	nNode.numberOfKeys-=1;
                  	//write it back to the disk
                  	SetNonLeafNode(newChildEntry,nNode);
                  }
                  //find the index from where I need to shift
                  //nodePointer is my current page
                  NonLeafNode currNode;
                  GetNonLeafNode(nodePointer,currNode);
                  // we have space, so add the key and sort it, it will go into its rightful position
                  int newKeyIndex=keyIndex+1;
                  currNode.keys.insert(currNode.keys.begin()+newKeyIndex,newKey);
                  //increment the number of keys
                  currNode.numberOfKeys+=1;
                  //place the element in the corresponding pointer and move all the other pointers right, keyIndex+1 seems to be correct
                  currNode.pointers.insert(currNode.pointers.begin()+(newKeyIndex+1),newPointer);

                  //finally, set the newChildEntry to Null Page
                  newChildEntry=NullPage;
  			    SetNonLeafNode(nodePointer,currNode);
  			}
  		  }
  		  else
  		  {
  			  //if newChildEntry is null then I can just put the value and dont worry about the pointer
  			  //what if I have only one parent key and one left child node and I am inserting a new element that should go to right node
  			  //normal case: nothing to execute
  		  }
  	  }
  	  else if(type==LeafNodeType)
  	  {

        LeafNode lNode;
  		GetLeafNode(nodePointer,lNode);
  		int totalEntries=lNode.numberOfEntries;
  		//obviously, will never be greater than, but keeping it to be safe or will check later
  		if(totalEntries>=2*orderOfBTree)
  		{
  			//void * splitPage=malloc(PF_PAGE_SIZE);
  			int splitNodeLength=orderOfBTree;
  			//new right node that we are going to split
  			//add the entry to the end of lNode
  			LeafValue key;
  			int indexValue;
  			key=entry.keyValue;
  			//find the element and put the element in its proper location
  			locateNewLeafElement(lNode,key,indexValue);
  			lNode.keys.insert(lNode.keys.begin()+indexValue,entry);
  			lNode.numberOfEntries+=1;

  			LeafNode rNode;
  			//initial assignment
  			rNode.numberOfEntries=0;
  			rNode.extrabit=0;
  			rNode.next_page=NullPage;
  			rNode.prev_page=NullPage;
  			rNode.type=LeafNodeType;
  			totalEntries=lNode.numberOfEntries; // change
  			for(int indexOfNode=splitNodeLength;indexOfNode<totalEntries;indexOfNode++)
  			{
  				//insert all the elements after the dth location to rNode
  				 rNode.keys.push_back(lNode.keys[indexOfNode]);

  				 //reducing the number of entries is done below the loop
  				 lNode.numberOfEntries-=1;
  				 rNode.numberOfEntries+=1;

  			}
  			//delete all the other elements
  		    lNode.keys.erase(lNode.keys.begin()+splitNodeLength,lNode.keys.end());
            PF_FileHandle indexHandle;
            _pf->OpenFile(_indexFileName.c_str(),indexHandle);
            char page[PF_PAGE_SIZE];
            indexHandle.AppendPage(&page);
//            free(page);
            int indexOfNewPage=indexHandle.GetNumberOfPages()-1; // Index of the new page
            _pf->CloseFile(indexHandle);

  			rNode.next_page=lNode.next_page;
  			lNode.next_page=indexOfNewPage; //as I am going to append the rNode to end of the file
  			rNode.prev_page=nodePointer;
  			LeafNode cousinNode;//cousinNode is the node next to the right Node *bad Naming convention*
  			if(rNode.next_page!=NullPage)
  			{
  				GetLeafNode(rNode.next_page,cousinNode);
  				//changed the cousinNode's previous pointer to rNode's pageNumber
  				cousinNode.prev_page=indexOfNewPage;
  				SetLeafNode(rNode.next_page,cousinNode);
  			}
  			//now write all the leftNode, rightNode and cousinNode to disk
  			SetLeafNode(nodePointer,lNode);
  			SetLeafNode(lNode.next_page,rNode);
  			newChildEntry=lNode.next_page;
  		}
  		else
  		{
              //just add that t
              int indexValue=-1;
              LeafValue key;
              key = entry.keyValue;
  			locateNewLeafElement(lNode,key,indexValue);

  			lNode.keys.insert(lNode.keys.begin()+indexValue,entry);

  			newChildEntry=NullPage;
              lNode.numberOfEntries+=1;
              SetLeafNode(nodePointer,lNode);
  		}

  	  }
  	  return ix_failure;
    }


  RC IX_IndexHandle::locateNewLeafElement(LeafNode leafNode,LeafValue key,int &value)
  {
	  value = 0;
	  int noOfEntries=leafNode.numberOfEntries;
	  for(int index=0;index<noOfEntries;index++)
	  {
	    //what about duplicates?

		if((_type == TypeInt && key.keyInt>leafNode.keys[index].keyValue.keyInt) ||
			(_type == TypeReal && key.keyFloat>leafNode.keys[index].keyValue.keyFloat)||
			(_type == TypeVarChar && key.keyVarChar > leafNode.keys[index].keyValue.keyVarChar))
	    {
	    	value=index+1;
	    }
	  }
	  return ix_success;
  }

  RC IX_IndexHandle::printNonLeafNode(NonLeafNode node)
  {
	  for(int index = 0 ; index < node.keys.size(); index++)
	  {
		  if(_type==TypeInt)
		  {
			  cout<<" NonleafNode "<<" "<<node.keys[index].keyInt<<endl;
		  }
		  else if(_type==TypeReal)
		  {
			  cout<<" NonleafNode "<<" "<<node.keys[index].keyFloat<<endl;
		  }
		  else
		  {
			  cout<<" NonleafNode "<<" "<<node.keys[index].keyVarChar<<endl;
		  }
	  }
	  return ix_success;
  }
    RC IX_IndexHandle::printLeafNode(LeafNode node)
    {
    	for(int index = 0 ; index < node.keys.size(); index++)
		  {
    		if(_type==TypeInt)
			  {
    			cout<<" leafNode "<<" "<<node.keys[index].keyValue.keyInt<<endl;
			  }
			  else if(_type==TypeReal)
			  {
				  cout<<" leafNode "<<" "<<node.keys[index].keyValue.keyFloat<<endl;
			  }
			  else
			  {
				  cout<<" leafNode "<<" "<<node.keys[index].keyValue.keyVarChar<<endl;
			  }
		  }
    	return ix_success;
    }

RC IX_IndexHandle::GetLeafValue(void *key,LeafValue &value)
{
	if(_type==TypeInt)
		  writeToBuffer(&value.keyInt,key,0,0,size_int);
	  else if(_type==TypeReal)
		  writeToBuffer(&value.keyFloat,key,0,0,size_float);
	  else if(_type==TypeVarChar){
		  int length  = 0 ;
		  writeToBuffer(&length,key,0,0,size_int);
		  char str[length+1];
		  str[length]='\0';
		  writeToBuffer(&str,key,0,size_int,length);
		  value.keyVarChar = string(str);
	  }
	return ix_success;
}

  RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)  // Delete index entry
  {


	  //create entry type
	  LeafValue value;
	  GetLeafValue(key,value);
	  LeafEntry entry;
	  entry.rid = rid;
	  entry.keyValue = value;
	  NodePointer oldchild = NullPage;
	  RC rc = Delete(NullPage,_headerInfo.rootPage,entry,oldchild);
	  	  if(rc==ix_success)
	  	  {
	  		  //check for more entries with duplicates
	  		  while(rc==ix_success)
	  		  {
	  			  oldchild = NullPage;
	  			  rc = Delete(NullPage,_headerInfo.rootPage,entry,oldchild);
	  		  }
	  		  return ix_success;
	  	  }
	  	  else
	  	  {
	  		  return ix_failure; // which is a failure as even 1 delete did not occur
	  	  }

	  // if varchar is attribute type
	  // key format is <length of string><string>
  }

  //********* private functions

  RC IX_IndexHandle::Delete(NodePointer parentpointer, NodePointer nodepointer, LeafEntry entry, NodePointer &oldchildEntry)
  {
	  NodeType type;
	  getNodeType(nodepointer,type);
	  if(type == NonleafNodeType || type == RootNodeType)
	  {
		  int childpointer=NullPage ;
		  FindIndexInNode(nodepointer,childpointer,entry);
		  RC rc=Delete(nodepointer,childpointer,entry,oldchildEntry);
		  if(oldchildEntry==NullPage)
		  {
			  return rc;
		  }
		  else
		  {
			  NonLeafNode currentNode;
			  GetNonLeafNode(nodepointer,currentNode);
			  // remove old child entry from N
//			  for(unsigned int index = 1 ; index < currentNode.pointers.size() ; index++)
//			  {
//				  if(oldchildEntry == currentNode.pointers[index])
//				  {
//					  currentNode.keys.erase(currentNode.keys.begin()+index-1);
//					  currentNode.pointers.erase(currentNode.pointers.begin()+index);
//					  currentNode.numberOfKeys = currentNode.keys.size();
//					  break;
//				  }
//			  }
			  currentNode.keys.erase(currentNode.keys.begin()+oldchildEntry);
			  currentNode.pointers.erase(currentNode.pointers.begin()+oldchildEntry+1);
			  currentNode.numberOfKeys = currentNode.keys.size();
			  SetNonLeafNode(nodepointer,currentNode);
			  // if N has entries to spare
			  //  set oldchildentry to null return
			  if(currentNode.keys.size()>=orderOfBTree)
			  {
				  oldchildEntry = NullPage;
				  return ix_success;
			  }
			  else
			  {
				  if(parentpointer==NullPage) // current node being root
				  {
					  if(currentNode.keys.size()>0)
						  return ix_success;
					  else if(currentNode.keys.size()==0)
					  {
						  NodePointer child = currentNode.pointers[0];
						  NodeType type;
						  getNodeType(child,type);

						  if(type==NonleafNodeType)
						  {
							//make the child --> root since its the only child of the current root

							  _headerInfo.rootPage = child;
							  SetHeaderInfo(_headerInfo);
							  NonLeafNode newroot;
							  GetNonLeafNode(child,newroot);
							  newroot.type = RootNodeType;
							  SetNonLeafNode(child,newroot);
						  }

					  }
					  // need to take care of the condition where number of nodes in root becomes zero
					  // and only child of the root node is the left child
					  // if child is leaf node then no change
					  // if child is nonleaf node then make the nonleaf node as the new root
				  }
				  else
				  {
					  NodePointer leftSibling, rightSibling,tempOldChildPointer;
					  FindSiblingsForParent(parentpointer,nodepointer,leftSibling,rightSibling,tempOldChildPointer);
					  if(leftSibling != NullPage)
					  {
						  NonLeafNode leftnode;
						  GetNonLeafNode(leftSibling,leftnode);
						  NonLeafNode parentnode;
						  GetNonLeafNode(parentpointer,parentnode);
						  if(leftnode.keys.size()>orderOfBTree)
						  {
							  //redistribute
							  oldchildEntry = NullPage;
							  int totalEntries = leftnode.keys.size()+currentNode.keys.size();
							  int leftentries = totalEntries/2;

							  LeafValue entryToBePushedUp = leftnode.keys[leftentries];
							  LeafValue entryToBePulledDown = parentnode.keys[tempOldChildPointer];
							  parentnode.keys[tempOldChildPointer] = entryToBePushedUp;
							  SetNonLeafNode(parentpointer,parentnode);

							  currentNode.keys.insert(currentNode.keys.begin(),entryToBePulledDown);
							  for(int index = leftnode.keys.size()-1 ; index>leftentries;index--)
							  {
								  currentNode.keys.insert(currentNode.keys.begin(),leftnode.keys[index]);
								  currentNode.pointers.insert(currentNode.pointers.begin(),leftnode.pointers[index]);
							  }

							  SetNonLeafNode(nodepointer,currentNode);
							  SetNonLeafNode(leftSibling,leftnode);
							  oldchildEntry = NullPage;
						  }
						  else
						  {
							  //merge
							  oldchildEntry = tempOldChildPointer;
							  LeafValue entryToBePulledDown = parentnode.keys[tempOldChildPointer];

							  leftnode.keys.push_back(entryToBePulledDown);

							  for(unsigned int index = 0 ; index < currentNode.keys.size() ; index++)
							  {
								  leftnode.keys.push_back(currentNode.keys[index]);
								  leftnode.pointers.push_back(currentNode.pointers[index]);
							  }
							  leftnode.pointers.push_back(currentNode.pointers.back());

							  SetNonLeafNode(leftSibling,leftnode);
						  }

					  }else if(rightSibling != NullPage)
					  {
						  NonLeafNode rightnode;
						  GetNonLeafNode(rightSibling,rightnode);
						  NonLeafNode parentnode;
						  GetNonLeafNode(parentpointer,parentnode);
						  if(rightnode.keys.size()>orderOfBTree)
						  {
							  //redistribute
							  oldchildEntry = NullPage;
							  int totalEntries = rightnode.keys.size()+currentNode.keys.size();
							  int currententries = totalEntries/2;
							  int rightentries = totalEntries - currententries;

							  LeafValue entryToBePushedUp = rightnode.keys[rightnode.keys.size()-1-rightentries];
							  LeafValue entryToBePulledDown = parentnode.keys[tempOldChildPointer];
							  parentnode.keys[tempOldChildPointer] = entryToBePushedUp;
							  SetNonLeafNode(parentpointer,parentnode);

							  currentNode.keys.push_back(entryToBePulledDown);
							  for(unsigned int index = 0 ; index<rightnode.keys.size()-1-rightentries;index++)
							  {
								  currentNode.keys.push_back(rightnode.keys[index]);
								  currentNode.pointers.push_back(rightnode.pointers[index]);
							  }
							  currentNode.pointers.push_back(rightnode.pointers[rightnode.keys.size()-1-rightentries]);

							  SetNonLeafNode(nodepointer,currentNode);
							  SetNonLeafNode(rightSibling,rightnode);
							  oldchildEntry = NullPage;
						  }
						  else
						  {
							  //merge
							  oldchildEntry = tempOldChildPointer;
							  LeafValue entryToBePulledDown = parentnode.keys[tempOldChildPointer];
							  currentNode.keys.push_back(entryToBePulledDown);

							  for(unsigned int index = 0 ; index < rightnode.keys.size() ; index++)
							  {
								  currentNode.keys.push_back(rightnode.keys[index]);
								  currentNode.pointers.push_back(rightnode.pointers[index]);
							  }
							  currentNode.pointers.push_back(rightnode.pointers.back());

							  SetNonLeafNode(nodepointer,currentNode);
						  }
					  }

				  }

			  }
		  }

	  }
	  else if(type == LeafNodeType)
	  {
		  LeafNode currentnode;
		  GetLeafNode(nodepointer,currentnode);
		  bool deleteFlag=false;
		  for(unsigned int index = 0 ; index < currentnode.keys.size();index++)
		  {
			  if((_type==TypeInt && currentnode.keys[index].keyValue.keyInt == entry.keyValue.keyInt) ||
				  (_type==TypeReal && currentnode.keys[index].keyValue.keyFloat == entry.keyValue.keyFloat) ||
				  (_type==TypeVarChar && currentnode.keys[index].keyValue.keyVarChar == entry.keyValue.keyVarChar))
			  {
				  currentnode.keys.erase(currentnode.keys.begin()+index);
				  currentnode.numberOfEntries = currentnode.keys.size();
				  deleteFlag = true;
				  break;
			  }
		  }
		  if(!deleteFlag) return ix_failure; // trying to delete an unavailable entry
		  SetLeafNode(nodepointer,currentnode);
		  NonLeafNode parentnode;
		  GetNonLeafNode(parentpointer,parentnode);

		  if(currentnode.keys.size()>=orderOfBTree)
		  {
			  oldchildEntry = NullPage;
			  return ix_success;
		  }
		  else if(parentnode.type == RootNodeType && parentnode.keys.size()==0)
		  {
			  // change
			  return ix_success;
		  }
		  else
		  {
			  // Identify left and right siblings if present
			  NodePointer leftSibling=NullPage, rightSibling=NullPage,tempOldChildPointer=NullPage;
			  FindSiblingsForParent(parentpointer,nodepointer,leftSibling,rightSibling,tempOldChildPointer);
			  if(leftSibling!=NullPage)
			  {
				  LeafNode leftnode;
				  GetLeafNode(leftSibling,leftnode);
				  if(leftnode.keys.size()>orderOfBTree)
				  {
					  //redistribute
					  // since the current node is only short of orderofTree by margin 1
					  // transferring 1 element from left sibling shall be enough
					  int totalEntries = leftnode.keys.size()+currentnode.keys.size();
					  int leftentries = totalEntries/2;

					  LeafValue entryTobeReplacedUp = leftnode.keys[leftentries].keyValue;

					  int lastIndexLeftNode = leftnode.keys.size()-1;
					  for(int index=lastIndexLeftNode;index>=leftentries;index--)
					  {
						  currentnode.keys.insert(currentnode.keys.begin(),leftnode.keys.back());
						  leftnode.keys.pop_back();
					  }

					  parentnode.keys[tempOldChildPointer]= entryTobeReplacedUp;
					  SetNonLeafNode(parentpointer,parentnode);
					  currentnode.numberOfEntries = currentnode.keys.size();
					  SetLeafNode(nodepointer,currentnode);
					  leftnode.numberOfEntries = leftnode.keys.size();
					  SetLeafNode(leftSibling,leftnode);
					  oldchildEntry = NullPage;
					  // no need to adjust pointers
				  }
				  else
				  {
					  //merge
					  // leftnode.keys.size() must be equal to orderofbtree
					  oldchildEntry = tempOldChildPointer; // since current node is the right child M
					  for(unsigned int index=0;index<currentnode.keys.size();index++)
					  {
						  leftnode.keys.push_back(currentnode.keys[index]);
					  }
					  leftnode.next_page = currentnode.next_page;
					  if(currentnode.next_page != NullPage)
					  {
						  LeafNode nextnode;
						  GetLeafNode(currentnode.next_page,nextnode);
						  nextnode.prev_page = leftSibling;
						  SetLeafNode(currentnode.next_page,nextnode);
					  }
					  else {}// nothing to do.
					  SetLeafNode(leftSibling,leftnode);
				  }
			  }
			  else if(rightSibling!=NullPage)
			  {
				  LeafNode rightnode;
				  GetLeafNode(rightSibling,rightnode);
				  if(rightnode.keys.size()>orderOfBTree)
				  {
					  //redistribute
					  // since the current node is only short of orderofTree by margin 1
					  // transferring 1 element from right sibling shall be enough
					  currentnode.keys.push_back(rightnode.keys[0]);
					  currentnode.numberOfEntries = currentnode.keys.size();
					  SetLeafNode(nodepointer,currentnode);
					  rightnode.keys.erase(rightnode.keys.begin()); // last element
					  rightnode.numberOfEntries = rightnode.keys.size();
					  SetLeafNode(rightSibling,rightnode);
					  oldchildEntry = NullPage;

					  int totalEntries = rightnode.keys.size()+currentnode.keys.size();
					  int currententries = totalEntries/2;
					  int rightentries = totalEntries - currententries;
					  int rightIndex = rightnode.keys.size()-rightentries;
					  LeafValue entryTobeReplacedUp = rightnode.keys[rightIndex].keyValue;

					  parentnode.keys[tempOldChildPointer]=entryTobeReplacedUp;
					  SetNonLeafNode(parentpointer,parentnode);

					  for(unsigned int index=0;index<=rightnode.keys.size()-rightentries;index++)
					  {
						  currentnode.keys.push_back(rightnode.keys[0]);
						  rightnode.keys.erase(rightnode.keys.begin());
					  }

					  currentnode.numberOfEntries = currentnode.keys.size();
					  SetLeafNode(nodepointer,currentnode);
					  rightnode.numberOfEntries = rightnode.keys.size();
					  SetLeafNode(rightSibling,rightnode);
					  oldchildEntry = NullPage;
				  }
				  else
				  {
					  //merge
					  // rightnode.keys.size() must be equal to orderofbtree
					  oldchildEntry = tempOldChildPointer; // right child here is the M
					  for(unsigned int index=0;index<rightnode.keys.size();index++)
					  {
						  currentnode.keys.push_back(rightnode.keys[index]);
					  }
					  currentnode.next_page = rightnode.next_page;
					  if(rightnode.next_page != NullPage)
					  {
						  LeafNode nextnode;
						  GetLeafNode(rightnode.next_page,nextnode);
						  nextnode.prev_page = nodepointer;
						  SetLeafNode(nodepointer,nextnode);
					  }
					  else {}// nothing to do.
					  SetLeafNode(nodepointer,currentnode);
				  }
			  }
			  else
			  {
				  //special case when root has only one child which is a leaf page
			  }

		  }
	  }
	  return ix_success;
  }

  RC IX_IndexHandle::FindPointerInNode(NodePointer nodepointer,int &index,LeafEntry entry)
	{
		NonLeafNode node;
		int keyIndex=0;
		GetNonLeafNode(nodepointer,node);
		//if no elements are present in the root
		index = node.pointers[0];
		//just a check
		//what if my new key is greater than all the elements? I am taking the left pointer of the last node which is wrong, so made changes
		int totalKeys=node.keys.size();
		for(; keyIndex < totalKeys; keyIndex++)
		{
			//what should I do for equality condition?
			if(	(_type==TypeInt && entry.keyValue.keyInt > node.keys[keyIndex].keyInt) ||
				(_type==TypeReal && entry.keyValue.keyFloat > node.keys[keyIndex].keyFloat) ||
				(_type==TypeVarChar && entry.keyValue.keyVarChar > node.keys[keyIndex].keyVarChar))
			{

				index = node.pointers[keyIndex+1];
			}
			else
			{
				// already found my index
				break;
			}
		}
		return ((node.keys.size()==0)?-1:
				(keyIndex==node.keys.size())?keyIndex-1:keyIndex);
	}

  RC IX_IndexHandle::FindSiblingsForParent(NodePointer parent,NodePointer nodepointer,NodePointer &leftSibling,NodePointer &rightSibling,int &tempOldChildPointer)
  {
	  if(parent==NullPage) return ix_success;// nodepointer is pointing to root
	  NonLeafNode parentNode;
	  GetNonLeafNode(parent,parentNode);
	  int pointerSize = parentNode.pointers.size();
	  for(int index = 0 ; index<parentNode.pointers.size();index++)
	  {
		  if(nodepointer==parentNode.pointers[index])
		  {
//			  tempOldChildPointer = parentNode.keys[index-1];
			  int dummy = index;
			  if(index<parentNode.keys.size())
			  {
				  tempOldChildPointer = index;
				  rightSibling = parentNode.pointers[index + 1];
			  }
			  else if(index>=1)
			  {
				  tempOldChildPointer = index - 1;
				  leftSibling = parentNode.pointers[index-1];
			  }

			  break;
		  }
		  else
		  {
//					  // Page should have been present.. corrupted node
//					  return ix_failure;
		  }
	  }
	  return ix_success;
  }

  RC IX_IndexHandle::find(LeafValue key,NodePointer &leafnode)
  {

	  tree_search(_headerInfo.rootPage,key,leafnode);
	  return ix_success;
  }
  RC IX_IndexHandle::tree_search(NodePointer node,LeafValue key,NodePointer &leafnode)
  {
	  NodeType type;
	  getNodeType(node,type);
	  if(type==LeafNodeType)
	  {
		leafnode = node;
	  }
	  else
	  {
		NonLeafNode currentNode;
		GetNonLeafNode(node,currentNode);
		node = currentNode.pointers[0];
		for(unsigned int index = 0 ; index < currentNode.keys.size() ; index++ )
		{
			if((_type==TypeInt && key.keyInt>currentNode.keys[index].keyInt)||
				(_type==TypeReal && key.keyFloat>currentNode.keys[index].keyFloat)||
				(_type==TypeVarChar && key.keyVarChar >currentNode.keys[index].keyVarChar))
			{
				node = currentNode.pointers[index+1];
			}
			else
			{
				break;
			}
		}
		tree_search(node,key,leafnode);

	  }
	  return ix_success;
  }

  RC IX_IndexHandle::setIndexFileName(string indexFileName)
  {
	  _indexFileName = indexFileName;
	  return ix_success;
  }

  RC IX_IndexHandle::GetHeaderInfo(HeaderInfo &info)
  {
	  PF_FileHandle indexHandle;
	  _pf->OpenFile(_indexFileName.c_str(),indexHandle);

	  char page[PF_PAGE_SIZE];
	  indexHandle.ReadPage(headerPageOffset,&page);

	  //structure <4 bytes extra bit><offset to root><order of tree>
	  int offset=startOfPage; // 4 bytes extra bit - now these will be used for storing index attrtype
	  writeToBuffer(&info.attrType,&page,0,offset,size_int);
	  offset+=size_int;
	  writeToBuffer(&info.rootPage,&page,0,offset,size_int);
	  offset+=size_int;
	  writeToBuffer(&info.orderOfBTree,&page,0,offset,size_int);
	  _pf->CloseFile(indexHandle);
	  return ix_success;
  }

  RC IX_IndexHandle::SetHeaderInfo(HeaderInfo &info)
  {
	  PF_FileHandle indexHandle;
	  _pf->OpenFile(_indexFileName.c_str(),indexHandle);
	  char page[PF_PAGE_SIZE];
	  indexHandle.ReadPage(headerPageOffset,&page);

	  //structure <4 bytes extra bit><offset to root><order of tree>
	  int offset=startOfPage; // 4 bytes extra bit - now these will be used for storing index attrtype
	  writeToBuffer(&page,&info.attrType,offset,0,size_int);
	  offset+=size_int;
	  writeToBuffer(&page,&info.rootPage,offset,0,size_int);
	  offset+=size_int;
	  writeToBuffer(&page,&info.orderOfBTree,offset,0,size_int);
	  indexHandle.WritePage(headerPageOffset,&page);
	  _pf->CloseFile(indexHandle);
	  return ix_success;
  }

  RC IX_IndexHandle::GetLeafNode(const NodePointer pagenum,LeafNode &node)
  {
	  //structure <key1 rid1><key2 rid2>...<keyn ridn>
	  PF_FileHandle indexHandle;
	  _pf->OpenFile(_indexFileName.c_str(),indexHandle);

	  // read the page
	  char page[PF_PAGE_SIZE];
	  indexHandle.ReadPage(pagenum,&page);

	  //read all the control bits
	  node.numberOfEntries = 0 , node.next_page = 0 , node.prev_page = 0 ;

	  int offset = endOfPage;
	  offset -= size_int;
	  writeToBuffer(&node.next_page,&page,0,offset,size_int); // a
	  offset -= size_int;
	  writeToBuffer(&node.prev_page,&page,0,offset,size_int); // b
	  offset -= size_int;
	  writeToBuffer(&node.numberOfEntries,&page,0,offset,size_int); // c
	  offset -= size_int;
	  writeToBuffer(&node.type,&page,0,offset,size_int); // d
	  offset -= size_int;
	  writeToBuffer(&node.extrabit,&page,0,offset,size_int); // e

	  //read all the keys into a vector
	  offset = startOfPage; // start of page
	  for(int index=0 ; index < node.numberOfEntries;index++)
	  {
		  LeafEntry entry;
		  LeafValue value;
		  if(_type==TypeInt){
			  writeToBuffer(&value.keyInt,&page,0,offset,size_int);
			  offset += size_int;
		  }
		  else if(_type==TypeReal){
			  writeToBuffer(&value.keyFloat,&page,0,offset,size_float);
			  offset += size_float;
		  }else if(_type==TypeVarChar)
		  {
			  int length = 0;
			  writeToBuffer(&length,&page,0,offset,size_int);
			  char str[length+1];
			  offset+=size_int;
			  writeToBuffer(&str,&page,0,offset,length);
			  offset += length;
			  str[length]='\0';
			  value.keyVarChar=string(str);
		  }
		  entry.keyValue = value;
		  writeToBuffer(&entry.rid.pageNum,&page,0,offset,size_int);
		  offset += size_int;
		  writeToBuffer(&entry.rid.slotNum,&page,0,offset,size_int);
		  offset += size_int;
		  node.keys.push_back(entry);
	  }
	  _pf->CloseFile(indexHandle);
	  return ix_success;
  }

  RC IX_IndexHandle::SetLeafNode(const NodePointer pagenum,LeafNode &node)
  {
	  //structure <key1 rid1><key2 rid2>...<keyn ridn>
	  PF_FileHandle indexHandle;
	  _pf->OpenFile(_indexFileName.c_str(),indexHandle);

	  // read the page
	  char page[PF_PAGE_SIZE];
	  indexHandle.ReadPage(pagenum,&page);
	  // insert all the keys
	  int offset = startOfPage; // start of the page
	  LeafEntry entry;
	  for(unsigned index = 0 ; index < node.keys.size() ; index++)
	  {
		  entry = node.keys[index];
		  LeafValue value = entry.keyValue;
		  if(_type==TypeInt){
			  writeToBuffer(&page,&value.keyInt,offset,0,size_int);
			  offset += size_int;
		  }
		  else if(_type==TypeReal){
			  writeToBuffer(&page,&value.keyFloat,offset,0,size_float);
			  offset += size_float;
		  }else if(_type==TypeVarChar)
		  {
			  int length = value.keyVarChar.length();
			  writeToBuffer(&page,&length,offset,0,size_int);
			  offset+=size_int;
			  writeToBuffer(&page,value.keyVarChar.c_str(),offset,0,length);
			  offset+=length;
		  }
			  writeToBuffer(&page,&entry.rid.pageNum,offset,0,size_int);
			  offset+=size_int;
			  writeToBuffer(&page,&entry.rid.slotNum,offset,0,size_int);
			  offset+=size_int;
		}
	  // insert the control bits at end of page
	  node.numberOfEntries = node.keys.size(); // *** small hack
	  offset = endOfPage;
	  offset = offset - size_int;
	  writeToBuffer(&page,&node.next_page,offset,0,size_int); // a
	  offset-=size_int;
	  writeToBuffer(&page,&node.prev_page,offset,0,size_int); // b
	  offset-=size_int;
	  writeToBuffer(&page,&node.numberOfEntries,offset,0,size_int); // c
	  offset-=size_int;
	  writeToBuffer(&page,&node.type,offset,0,size_int);
	  offset-=size_int;
	  writeToBuffer(&page,&node.extrabit,offset,0,size_int);

	  // write page to file
	  indexHandle.WritePage(pagenum,&page);
	  _pf->CloseFile(indexHandle);
	  return ix_success;
  }

  RC IX_IndexHandle::GetNonLeafNode(const NodePointer pagenum,NonLeafNode &node)
    {
  	  PF_FileHandle fileHandle;
  	  _pf->OpenFile(_indexFileName.c_str(),fileHandle);
  	  char page[PF_PAGE_SIZE];
  	  fileHandle.ReadPage(pagenum,&page);
  	  _pf->CloseFile(fileHandle);
  	  int offset=PF_PAGE_SIZE;
  	  offset-=size_int;
  	  writeToBuffer(&node.extraBit1,&page,0,offset,size_int);
  	  offset-=size_int;
  	  writeToBuffer(&node.extraBit2,&page,0,offset,size_int);
  	  offset-=size_int;
  	  writeToBuffer(&node.numberOfKeys,&page,0,offset,size_int);
  	  offset-=size_int;
  	  writeToBuffer(&node.type,&page,0,offset,size_int);
  	  offset-=size_int;
  	  writeToBuffer(&node.extrabit,&page,0,offset,size_int);

  	  offset = startOfPage;
  	  for(int index = 0 ; index<node.numberOfKeys; index++)
	  {
  		  LeafValue currentValue;
  		  if(_type==TypeInt){
  			  writeToBuffer(&currentValue.keyInt,&page,0,offset,size_int);
  			offset += size_int;
  		  }
  		  else if(_type==TypeReal){
  			writeToBuffer(&currentValue.keyFloat,&page,0,offset,size_float);
  			offset += size_float;
  		  }
  		  else if(_type==TypeVarChar){
  			  int length = 0 ;
  			  writeToBuffer(&length,&page,0,offset,size_int);
  			  offset+=size_int;
  			  char str[length+1];
  			  str[length]='\0';
  			  writeToBuffer(&str,&page,0,offset,length);
  			  offset+=length;
  			  currentValue.keyVarChar = string(str);
  		  }
  		  node.keys.push_back(currentValue);
	  }

  	  for(int index = 0 ; index<node.numberOfKeys+1;index++)
  	  {
  		  int currentPointer = 0;
  		  writeToBuffer(&currentPointer,&page,0,offset,size_int);
  		  node.pointers.push_back(currentPointer);
  		  offset += size_int;
  	  }
  	  return ix_success;
    }

    RC IX_IndexHandle::SetNonLeafNode(const NodePointer pagenum,NonLeafNode &node)
    {
    	char page[PF_PAGE_SIZE];
  	  int offset=PF_PAGE_SIZE;
  	  node.numberOfKeys = node.keys.size();
  	  node.extraBit1=0;
  	  offset-=size_int;
  	  writeToBuffer(&page,&node.extraBit1,offset,0,size_int);
  	  node.extraBit2=0;
  	  offset-=size_int;
  	  writeToBuffer(&page,&node.extraBit2,offset,0,size_int);
  	  offset-=size_int;
  	  writeToBuffer(&page,&node.numberOfKeys,offset,0,size_int);
  	  offset-=size_int;
  	  writeToBuffer(&page,&node.type,offset,0,size_int);
  	  node.extrabit=0;
  	  offset-=size_int;
  	  writeToBuffer(&page,&node.extrabit,offset,0,size_int);
        // enter the keys at the beginning of the page
  	  unsigned int i;
  	  offset = startOfPage;
  	  for(i=0;i<node.keys.size();i++)
      {
  		LeafValue currentValue = node.keys[i];
  		if(_type==TypeInt){
  			writeToBuffer(&page,&currentValue.keyInt,offset,0,size_int);
  			offset += size_int;
		  }
		  else if(_type==TypeReal){
			writeToBuffer(&page,&currentValue.keyFloat,offset,0,size_float);
			offset += size_float;
		  }
		  else if(_type==TypeVarChar){
			  int length = currentValue.keyVarChar.length() ;
			  writeToBuffer(&page,&length,offset,0,size_int);
			  offset+=size_int;
			  writeToBuffer(&page,currentValue.keyVarChar.c_str(),offset,0,length);
			  offset+=length;
		  }
       }

  	  for(unsigned int j=0;j<node.pointers.size();j++)
  	  {
		   writeToBuffer(&page,&node.pointers[j],offset,0,size_int);
		   offset+=size_int;
  	  }
  	  PF_FileHandle fileHandle;
  	  _pf->OpenFile(_indexFileName.c_str(),fileHandle);
  	  fileHandle.WritePage(pagenum,&page);
  	  _pf->CloseFile(fileHandle);

  	  return ix_success;
    }

    RC IX_IndexHandle::FindIndexInNode(NodePointer nodepointer,int &index,LeafEntry entry)
    {
    	NonLeafNode node;
    	GetNonLeafNode(nodepointer,node);
    	index = node.pointers[0];
    	int numberOfNodeKeys = node.keys.size();
    	for(int keyIndex = 0; keyIndex < numberOfNodeKeys; keyIndex++)
    	{
    		if((_type==TypeInt && entry.keyValue.keyInt > node.keys[keyIndex].keyInt) ||
    			(_type==TypeReal && entry.keyValue.keyFloat > node.keys[keyIndex].keyFloat)||
    			(_type==TypeVarChar && entry.keyValue.keyVarChar > node.keys[keyIndex].keyVarChar))
			{
    			index = node.pointers[keyIndex+1];
    		}
    		else
    		{
    			// already found my index
    			break;
    		}
    	}
    	return ix_success;
    }

    RC IX_IndexHandle::getNodeType(const int pageNum,NodeType &type)
     {
   	  PF_FileHandle fileHandle;
   	  _pf->OpenFile(_indexFileName.c_str(),fileHandle);
      char page[PF_PAGE_SIZE];
   	  fileHandle.ReadPage(pageNum,&page);
   	  _pf->CloseFile(fileHandle);
   	  writeToBuffer(&type,&page,0,PF_PAGE_SIZE-4*size_int,size_int);
   	  return ix_success;
     }



  RC IX_IndexHandle::writeToBuffer(const void* buffer,const void* data, int offset1,int offset2,int length)
  {
  	memcpy((char *)buffer+offset1,(char *)data+offset2,length);
  	return ix_success;
  }

  //**************************************************

  //***************** Index Scan *********************

  IX_IndexScan::IX_IndexScan(){}  								// Constructor
  IX_IndexScan::~IX_IndexScan(){}								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value)
  {
	  	  PF_Manager *pf;
	  	  pf = PF_Manager::Instance();
	  	  if(pf->fileExists(indexHandle._indexFileName.c_str()))
	  	  {
	  		  _indexHandle=indexHandle;
	  		  _compOp = compOp;
	  		  RM *rm= RM::Instance();
	  		  Attribute attr;
	  		  rm->readAttributeType(_indexHandle._tableName,_indexHandle._attributeName,attr);
	  		  _attrType = attr.type;
	  		  if(_compOp != NO_OP)
	  		  {
	  			  if(_attrType == TypeInt)
	  				  _indexHandle.writeToBuffer(&_valueInt,value,0,0,size_int);
	  			  else if(_attrType == TypeReal)
	  				_indexHandle.writeToBuffer(&_valueFloat,value,0,0,size_float);
	  			  else
	  			  {
	  				  int length = 0;
	  				_indexHandle.writeToBuffer(&length,value,0,0,size_int);
	  				char str[length+1];
	  				_indexHandle.writeToBuffer(&str,value,0,size_int,length);
	  				str[length]='\0';
	  				_valueVarChar = string(str);
	  			  }
	  		  }
	  		  else
	  		  {
	  			  //value doesn't matter
	  		  }
	  		  rids.clear();
	  		  nextVisitedNode = NullPage;
	  		  _isScanOpen = true;
	  		  return ix_success;
	  	  }
	  	  else
	  	  {
	  		  return ix_filenotfound;
	  	  }
  }

  RC IX_IndexScan::GetNextEntry(RID &rid)  // Get next matching entry
  {
	  if(!_isScanOpen) return ix_failure;
	  if(nextVisitedNode==NullPage)
	  {
		  HeaderInfo info;
		  _indexHandle.GetHeaderInfo(info);
		  NodeType type = NonleafNodeType;
		  nextVisitedNode = info.rootPage;
		  while(type!=LeafNodeType)
		  {
			  NonLeafNode node;
			  _indexHandle.GetNonLeafNode(nextVisitedNode,node);
			  nextVisitedNode = node.pointers[0];
			  _indexHandle.getNodeType(nextVisitedNode,type);
		  }
	  }
	  if(rids.size()!=0)
	  {
		  // somewhere in between of scan
		  rid = rids.back();
		  rids.pop_back();
	  }
	  else if(nextVisitedNode == EndOfFile){
		  return ix_endoffile;
	  }
	  else
	  {
		  while(nextVisitedNode != EndOfFile)
		  {
			  LeafNode node;
			  _indexHandle.GetLeafNode(nextVisitedNode,node);
			  int nodeSize = node.keys.size();
			  if(nodeSize==0) return ix_failure; // no elements in the node probably referring to root with one child leaf node and node does not contain any elements aka Start state
			  for(int index = 0 ; index < nodeSize ; index++)
				{
				  LeafValue value;
				  value = node.keys[index].keyValue;
				  bool valueFound  = false;
				  switch(_compOp)
					  {
						case EQ_OP:
							if((_attrType == TypeInt && value.keyInt == _valueInt) ||
								(_attrType == TypeReal && value.keyFloat == _valueFloat)||
								(_attrType == TypeVarChar && value.keyVarChar == _valueVarChar)) valueFound = true;
							  break;
						case LT_OP:
							if((_attrType == TypeInt && value.keyInt < _valueInt) ||
								(_attrType == TypeReal && value.keyFloat < _valueFloat)||
								(_attrType == TypeVarChar && value.keyVarChar < _valueVarChar)) valueFound = true;
							  break;
						case GT_OP:
							if((_attrType == TypeInt && value.keyInt > _valueInt) ||
								(_attrType == TypeReal && value.keyFloat > _valueFloat)||
								(_attrType == TypeVarChar && value.keyVarChar > _valueVarChar)) valueFound = true;
								break;
						case LE_OP:
							if((_attrType == TypeInt && value.keyInt <= _valueInt) ||
								(_attrType == TypeReal && value.keyFloat <= _valueFloat)||
								(_attrType == TypeVarChar && value.keyVarChar <= _valueVarChar)) valueFound = true;
							   break;
						case GE_OP:
							if((_attrType == TypeInt && value.keyInt >= _valueInt) ||
								(_attrType == TypeReal && value.keyFloat >= _valueFloat)||
								(_attrType == TypeVarChar && value.keyVarChar >= _valueVarChar)) valueFound = true;
								break;
						case NE_OP:
							if((_attrType == TypeInt && value.keyInt != _valueInt) ||
								(_attrType == TypeReal && value.keyFloat != _valueFloat)||
								(_attrType == TypeVarChar && value.keyVarChar != _valueVarChar)) valueFound = true;
								break;
						case NO_OP:
							valueFound = true;
							  break;
					  }
					if(valueFound)
					{
						rids.push_back(node.keys[index].rid);
					}
					if(index == nodeSize-1)
					{
						if(node.next_page!=NullPage)
						{
							nextVisitedNode = node.next_page;
							LeafNode nextLeafNode ;
							_indexHandle.GetLeafNode(nextVisitedNode,nextLeafNode);
							node = nextLeafNode;
							nodeSize = node.keys.size();
		//						_nextIndex = 0 ;
							index = -1; // index++ will make it index = 0
						}
						else
						{
							// reached end of file
							nextVisitedNode = EndOfFile;
		//						return ix_endoffile;
						}
//						if(valueFound) break;
					}
				}// enf of for
		  }// end of while
		  if(rids.size()!=0){
			  rid = rids.back();
			  rids.pop_back();
		  }else if(nextVisitedNode==EndOfFile)
		  {
			  // no rids found
			  return ix_endoffile;
		  }
	  }// end of else
	return ix_success;
  }

  RC IX_IndexScan::CloseScan()             // Terminate index scan
  {
	  nextVisitedNode = EndOfFile;
	  _isScanOpen = false;
	  rids.clear();
	  return ix_success;
  }

  //******************************************

  //******************** Miscellaneous Functions

  void IX_PrintError (RC rc)
  {

		if(rc==ix_success)
			  cout<<"Success"<<endl;
		else if(rc==ix_failure)
			  cout<<"Failure"<<endl;
		else if(rc==ix_endoffile)
			  cout<<"Scan has reached end of file"<<endl;
		else if(rc==ix_filenotfound)
			cout<<"File doesn't exist"<<endl;
  }

