
#include "rm.h"
const char *_catalog = "sys_catalog";
const char *_attribute = "sys_attribute";

const int failure = -1;
const int success = 0;
const int size_int = sizeof(int);
const int size_float = sizeof(float);
const int size_slot = sizeof(SlotEntry);
const int _typeInt = 0;
const int _typeReal = 1;
const int _typeVarChar = 2;
const int pageNotFound = -1; // since page numbers start from 0 , -1 signifies negative page aka page not found
const int _reservedHeaderPageBytes=3;
const int bufsize=200;

RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	_pf = PF_Manager::Instance();
	// create catalog if it already does not exist
	initialSetup();
}

RM::~RM()
{
	//free(_pf);
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
	if(_pf->fileExists(tableName.c_str()))
	{
	return failure;
	}
	// create a file <tableName>
	_pf->CreateFile(tableName.c_str());
	// add table entry to catalog <table_id table_name version>
	_pf->OpenFile(tableName.c_str(),fileHandle);
	char data[PF_PAGE_SIZE];
	headerPage(&data);
	fileHandle.AppendPage(&data);
	//free(data);
//	data = malloc(PF_PAGE_SIZE);
	dataPage(&data);
	fileHandle.AppendPage(&data);
	//free(data);
	_pf->CloseFile(fileHandle);
	PF_FileHandle catalogHandle;
	_pf->OpenFile(_catalog,catalogHandle);
	int max_size=0;
	for(unsigned i=0;i<attrs.size();i++)
	{
	max_size+=attrs[i].length;
	}
	char tuple[2*max_size];
//	data = malloc(PF_PAGE_SIZE);
	catalogHandle.ReadPage(0,&data);
	int table_id = -1,tuplesize=0;
	writeToBuffer(&table_id,&data,0,PF_PAGE_SIZE-2*size_int,size_int); // read the next table id say 3
	int next_table_id = table_id +1;
	writeToBuffer(&data,&next_table_id,PF_PAGE_SIZE-2*size_int,0,size_int); // write 4 back into the catalog table header page
	catalogHandle.WritePage(0,&data);
	//free(data);
	_pf->CloseFile(catalogHandle);
	prepareCatalogTuple(table_id,tableName,1.0,&tuple,&tuplesize);

	RC rc;
	RID rid;
	rc=insertTuple(_catalog,(char *)&tuple+size_int,rid);

	// add entry to attribute <id tableName field1 field2 field3 field4 ...>
	//*************** Small Hack ***** need to store all attributes of the table in a single page
	int attrs_size = attrs.size();
	for(int i =0;i<attrs_size;i++)
	{
	void *tuple = malloc(1000);
	prepareAttributeTuple(table_id,attrs[i].name,attrs[i].type,attrs[i].length,tuple,&tuplesize);
	rc=insertTuple(_attribute,(char *)tuple+size_int,rid);

	}
	//close the file
	//free(data);
	return success;
}

RC RM::deleteTable(const string tableName)
{
	// remove all table entry from catalog <table_id table_name version>
	// remove all entries from tables_detail_info <id tableName field1 field2 field3 field4 ...>
	// remove the file <tableName.sql>
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}

	int table_id=-1;
	RID rid;
	findTableId(tableName,&table_id,rid);
	deleteTuple(_catalog,rid);
	_pf->DestroyFile(tableName.c_str());
	return success;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}
	//refer to catalog and retrieve the table_id
	int table_id = -1;
	RID rid;
	findTableId(tableName,&table_id,rid);
	// open the attribute file... retrieve all the data pages from the header page
	findAttributesforTable(table_id,attrs);
	//refer to tables_detail_info and fetch attributes
	return success;
}

RC RM::findTableId(const string tableName,int *table_id,RID &rid)
{
	// open the catalog file .. retrieve all the data pages from the header page
	*table_id = -1;
	vector<int> dataPages;
	getAllPagesInHeader(_catalog,dataPages);
	PF_FileHandle catalogHandle;
	_pf->OpenFile(_catalog,catalogHandle);
	// scan through all the pages one after the other and find the table id
	char page[PF_PAGE_SIZE];
	for(unsigned dataPageIndex=0; dataPageIndex<dataPages.size() ; dataPageIndex++)
	{
		catalogHandle.ReadPage(dataPages[dataPageIndex],&page);
		SlotsDirectory pageDirectory;
		getDirectoryList(&page,pageDirectory);
		for(unsigned slotIndex = 0 ; slotIndex<pageDirectory.list.size();slotIndex++)
		{
			SlotEntry slot;
			slot = pageDirectory.list[slotIndex];
			char tuple[slot.length+1]; // extra buffer

			//check if the record has been deleted or not.
			if(slot.offset <= 0 && slot.length <=0)
			{
				continue;
			}
			writeToBuffer(&tuple,&page,0,slot.offset,slot.length);
			int recordOffset=0,recordTable_id=-1;
			int tableNameSize=0;
			const void *recordTableName = malloc(1000);
			int _recordType = -2;
			writeToBuffer(&_recordType,&tuple,0,recordOffset,size_int);
			recordOffset += size_int; // to fetch the record_type
			writeToBuffer(&recordTable_id,&tuple,0,recordOffset,size_int);
			recordOffset = recordOffset+size_int;
			writeToBuffer(&tableNameSize,&tuple,0,recordOffset,size_int);
			recordOffset = recordOffset + size_int;
			writeToBuffer(recordTableName,&tuple,0,recordOffset,tableNameSize);
			//free(tuple);
			int tableLength = tableName.length();

			if((tableNameSize ==  tableLength) &&
			   memcmp(tableName.c_str(),recordTableName,tableNameSize)==0)
			{
				// record found ... initialize table_id
				*table_id = recordTable_id;
				rid.pageNum=dataPages[dataPageIndex];
				rid.slotNum=slot.offset;
				break;
			}

		}
		if(*table_id!=-1)
		{
			break;
		}
	}
	// close the catalog file
	_pf->CloseFile(catalogHandle);
	return success;
}

RC RM::findAttributesforTable(int table_id,vector<Attribute> &attrs)
{
	// open the attribute file .. retrieve all the data pages from the header page
	vector<int> dataPages;
	getAllPagesInHeader(_attribute,dataPages);
	PF_FileHandle attributeHandle;
	_pf->OpenFile(_attribute,attributeHandle);
	// scan through all the pages one after the other and locate the table id and find all the attributes
	char page[PF_PAGE_SIZE];
	for(unsigned dataPageIndex=0; dataPageIndex<dataPages.size() ; dataPageIndex++)
	{
		attributeHandle.ReadPage(dataPages[dataPageIndex],&page);
		SlotsDirectory pageDirectory;
		getDirectoryList(&page,pageDirectory);
		//printDirectory(pageDirectory);
		//cout<<dataPageIndex<<" @ "<<pageDirectory.list.size()<<endl;

		for(unsigned slotIndex = 0 ; slotIndex<pageDirectory.list.size();slotIndex++)
		{
		//	cout<<slotIndex<<"#"<<endl;
			SlotEntry slot;
			slot = pageDirectory.list[slotIndex];
			if(slot.offset<=0 && slot.length<=0)
			{
				continue;
			}
			char tuple[slot.length+1]; // extra buffer
			writeToBuffer(&tuple,&page,0,slot.offset,slot.length);

			int recordOffset=0,recordTable_id=-1;
			int _recordType = -2;
			writeToBuffer(&_recordType,&tuple,0,recordOffset,size_int);
			recordOffset += size_int;
			writeToBuffer(&recordTable_id,&tuple,0,recordOffset,size_int);
			recordOffset = recordOffset + size_int;
			//cout<<"$"<<table_id<<" & "<<recordTable_id<<endl;
			if(table_id == recordTable_id)
			{
				int columnNameSize = -1,type=0,length=-1;
				writeToBuffer(&columnNameSize,&tuple,0,recordOffset,size_int); // column name Length
				recordOffset = recordOffset+size_int;
				const void *name = malloc(columnNameSize+1);
				writeToBuffer(name,&tuple,0,recordOffset,columnNameSize);
				recordOffset += columnNameSize;
				writeToBuffer(&type,&tuple,0,recordOffset,size_int);
				recordOffset += size_int;
				writeToBuffer(&length,&tuple,0,recordOffset,size_int);
				recordOffset += size_int;
				*((char *)name+columnNameSize) = '\0';
				//free(tuple);
				Attribute attr;
				attr.length = length;
				attr.type = (AttrType)type;
				attr.name = string((char *)name);
				attrs.push_back(attr);
			}
		}

	}

	// close the catalog file
	_pf->CloseFile(attributeHandle);
	return success;
}

//  Format of the data passed into the function is the following:
//  1) data is a concatenation of values of the attributes
//  2) For int and real: use 4 bytes to store the value;
//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
RC RM::insertTuple(const string tableName, const void *data, RID &rid)
{
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}
	SlotsDirectory directory;
		//refer to the header page
		//int data_page_number = -1;
		int pageNumber = -1,offsetInPage = -1;
		//calculate the length of the record
		// For typeInt and typeReal just add size_int and size_float respectively
		// For varchar must check for length of the varchar
		int recordSize = 0; // This is for DataRecordBit
		getRecordLength(tableName,&recordSize,data);
		recordSize += size_int; //for the datarecord type bit

		//find the page with free space or add page if no free space is available then add a new page
		freeSpaceInTable(tableName,recordSize,&pageNumber,&offsetInPage);

		//open the file
		PF_FileHandle tableHandle;
		_pf->OpenFile(tableName.c_str(),tableHandle);
		char page[PF_PAGE_SIZE]; ;
		if(pageNumber == pageNotFound)
		{
//			page = malloc(PF_PAGE_SIZE);
			dataPage(&page);
			tableHandle.AppendPage(&page);
			//add this page to header page
			addPageToHeader(tableHandle);
			pageNumber = tableHandle.GetNumberOfPages()-1 ; // indexes start from 0
			offsetInPage = 0;

		}
//		page = malloc(PF_PAGE_SIZE);
		tableHandle.ReadPage(pageNumber,&page);
		SlotsDirectory pageDirectory;
		getDirectoryList(&page,pageDirectory);
		SlotEntry slot;
		slot.offset = offsetInPage ;
		slot.length = recordSize;
		rid.pageNum = pageNumber;
		if(offsetInPage == pageDirectory.freeSpaceOffset)
		{
			rid.slotNum = pageDirectory.list.size();
			pageDirectory.list.push_back(slot);
		}
		else
		{
			int furtherSlots = -1,remainingSpace = recordSize;
			for(unsigned i=0;i<pageDirectory.list.size();i++)
			{
				int _slotOffset = abs(pageDirectory.list[i].offset);
				if(furtherSlots != -1)
				{
					int _slotLength = abs(pageDirectory.list[i].length);
					int tempSpace = remainingSpace - _slotLength;
					pageDirectory.list[i].offset = (tempSpace>=0)?-1:(-1*pageDirectory.list[i].offset-tempSpace); // since originally offset is negative
					pageDirectory.list[i].length = (tempSpace>=0)?0:(-1*tempSpace);
					furtherSlots = (tempSpace<=0)?-1:furtherSlots+1;
					remainingSpace = tempSpace;
				}
				if(_slotOffset == offsetInPage)
				{
					rid.slotNum = i;
					int _length = abs(pageDirectory.list[i].length);
					remainingSpace = remainingSpace - _length;
					pageDirectory.list[i].offset = abs(pageDirectory.list[i].offset);
					pageDirectory.list[i].length = recordSize;
					furtherSlots = (remainingSpace>0)?0:-1;
				}
				if(remainingSpace<=0) break;
			}
			if(furtherSlots!=-1)
			{
				// take space from freespace at the end of page
				pageDirectory.freeSpaceOffset = pageDirectory.freeSpaceOffset + remainingSpace;
			}

		}
	//	writeToBuffer(directory.freeSpaceOffset)
		//insert the record

		//update the directory_lists
		int _recordType = (int)NaturalRecord;
		writeToBuffer(&page,&_recordType,offsetInPage,0,size_int);
		offsetInPage += size_int;
		recordSize = recordSize - size_int;
		writeToBuffer(&page,data,offsetInPage,0,recordSize);
		pageDirectory.freeSpaceOffset = (offsetInPage+recordSize>pageDirectory.freeSpaceOffset)?
										offsetInPage+recordSize:pageDirectory.freeSpaceOffset;
		insertDirectoryList(&page,pageDirectory);
		tableHandle.WritePage(pageNumber,&page);
		//close the file
		_pf->CloseFile(tableHandle);
		return success;

}

RC RM::addPageToHeader(PF_FileHandle &tableHandle)
{

// check if header page is full
// proceed to the header page with free data or insert headerpage
// else header page not full then find the offset  of page
// insert into the offset
	int currentInsertedPage=tableHandle.GetNumberOfPages()-1;
	char headerPageBuffer[PF_PAGE_SIZE];
	tableHandle.ReadPage(0,&headerPageBuffer);
	int offset=0;
	int numberOfPages=0;
	writeToBuffer(&numberOfPages,&headerPageBuffer,0,0,size_int);
	//last 4 bytes are filled with pointer to next header page(in case we implement) and 4 bytes for next Table id
	//and first for bytes for number of pages
	int totalNumberOfDataPages=(PF_PAGE_SIZE-_reservedHeaderPageBytes*size_int)/size_int;
	numberOfPages+=1;
	if(numberOfPages<totalNumberOfDataPages)
	{
		//because the page index starts from zero


		writeToBuffer(&headerPageBuffer,&currentInsertedPage,numberOfPages*size_int,0,size_int);
		writeToBuffer(&headerPageBuffer,&numberOfPages,0,0,size_int);
		tableHandle.WritePage(0,&headerPageBuffer);
	}
	else
	{

			int nextHeaderPage=0;
			writeToBuffer(&nextHeaderPage,&headerPageBuffer,0,PF_PAGE_SIZE-size_int,size_int);
			if(nextHeaderPage==0)
			{
				//create a new header page

				char data[PF_PAGE_SIZE];
				headerPage(&data);
				tableHandle.AppendPage(&data);
				nextHeaderPage=tableHandle.GetNumberOfPages()-1;
				writeToBuffer(&headerPageBuffer,&nextHeaderPage,PF_PAGE_SIZE-size_int,0,size_int);
				tableHandle.ReadPage(nextHeaderPage,&data);
				writeToBuffer(&data,&numberOfPages,size_int,0,size_int);
				tableHandle.WritePage(nextHeaderPage,&data);

			}
			else
			{
				//
				char data[PF_PAGE_SIZE];
				int pageNumberOfNextHeader=0;
				writeToBuffer(&pageNumberOfNextHeader,headerPageBuffer,0,PF_PAGE_SIZE-size_int,size_int);
				tableHandle.ReadPage(pageNumberOfNextHeader,&data);
				int newNumberOfPages=0;
				writeToBuffer(&newNumberOfPages,&data,0,0,size_int);
				newNumberOfPages+=1;
				writeToBuffer(&data,&currentInsertedPage,newNumberOfPages*size_int,0,size_int);
				writeToBuffer(&data,&newNumberOfPages,0,0,size_int);
				tableHandle.WritePage(pageNumberOfNextHeader,&data);

			}

	}

	return success;
}

RC RM::deleteTuples(const string tableName)
{

	//***** Hack *** resetting the header page and first data page
	// truncate the page (//free)
	// open the file
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}

	vector<int> pages;
	getAllPagesInHeader(tableName,pages);

	PF_FileHandle tableHandle;
	_pf->OpenFile(tableName.c_str(),tableHandle);
	// add/write the first data page -- change resetting every data page
	char page[PF_PAGE_SIZE];
	dataPage(&page);
	for(unsigned i=0;i<pages.size();i++)
	{
		tableHandle.WritePage(pages[i],&page);
	}
	// add/write header page

	//delete tuples is this correct??
	char page1[PF_PAGE_SIZE];
	headerPage(&page1);
	tableHandle.WritePage(0,&page1);
	// close the file
	_pf->CloseFile(tableHandle);
	return success;
}

RC RM::deleteTuple(const string tableName, const RID &rid)
{

	if(_pf->fileExists(tableName.c_str()))
	{
		//open the file
		PF_FileHandle tableHandle;
		_pf->OpenFile(tableName.c_str(),tableHandle);
		char page[PF_PAGE_SIZE];
		tableHandle.ReadPage(rid.pageNum,&page);
		SlotsDirectory pageDirectory;
		getDirectoryList(&page,pageDirectory);
		// making the offset to negative to indicate its a free space and preserving the length of the free space memory
		pageDirectory.list[rid.slotNum].offset = -1*pageDirectory.list[rid.slotNum].offset;
		pageDirectory.list[rid.slotNum].length = -1*pageDirectory.list[rid.slotNum].length;
		//update the directory list by changing the offset to -<original value> say 145 to -145
		//update the directory list by changing the length to -<original value> say 1224 to -1224 bytes
		insertDirectoryList(&page,pageDirectory);
		tableHandle.WritePage(rid.pageNum,&page);
		//close the file
		_pf->CloseFile(tableHandle);
	}

	return success;
}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{

	//similar to insert but extra cost of checking if there is an available space immediately after the record

	//********* calculate the record length **********
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}

	int recordSize = 0;
	getRecordLength(tableName,&recordSize,data);
	recordSize+=size_int;
	//************* end ***********************

	SlotEntry slot;
	SlotsDirectory pageDirectory;
	PF_FileHandle tableHandle;
	_pf->OpenFile(tableName.c_str(),tableHandle);
	char page[PF_PAGE_SIZE];
	tableHandle.ReadPage(rid.pageNum,&page);
	getDirectoryList(&page,pageDirectory);
	slot = pageDirectory.list[rid.slotNum];
	_pf->CloseFile(tableHandle);


	// After the tuple has been updated , Three cases
	// a. new record is smaller in size than prev record
	// b. new record is same in size as prev record
	// c. new record is larger in size than prev record

	// slot.length contains the length of the original record
	if(recordSize<=slot.length)
	{
		// Case a,b:
		// If record is smaller or same in size
		// then insert back into the exact same location ,
		// update the rid accordingly and write pagedirectory back to page
		pageDirectory.list[rid.slotNum].length = recordSize;
		writeToBuffer(&page,data,slot.offset+size_int,0,recordSize-size_int); // to eliminate the datarecordtypebit
		pageDirectory.list[rid.slotNum] = slot;
		_pf->OpenFile(tableName.c_str(),tableHandle);
		insertDirectoryList(&page,pageDirectory); // freespace offset is untouched here
		tableHandle.WritePage(rid.pageNum,&page);
		_pf->CloseFile(tableHandle);
	}
	else if(recordSize>slot.length)
	{
		// Case c:
		// If space is available immediately after the original record use this space
		// else find free space in table for the required length
		// use insertTuple to create the new record and then update the new rid as data in original record
		int spaceToBeFilled = recordSize - slot.length,numberOfSlots = 0;
		SlotEntry tempSlot;
		for(unsigned int index = rid.slotNum+1;index<pageDirectory.list.size();index++)
		{
			tempSlot = pageDirectory.list[index];
			if(tempSlot.offset<=0 && tempSlot.length<=0)
			{
				numberOfSlots +=1;
				spaceToBeFilled = spaceToBeFilled - abs(tempSlot.length);
				if(spaceToBeFilled <=0)
				{
					pageDirectory.list[rid.slotNum].length = recordSize;
					break;
				}
				else
				{
					continue;
				}
			}
			else
			{
				//need to search for freespace elsewhere in table

				numberOfSlots = 0;
				break;
			}
		}

		if(spaceToBeFilled<=0)
		{
			for(int slotIndex = 1;slotIndex<numberOfSlots;slotIndex++)
			{
				int _index = rid.slotNum + slotIndex;
				pageDirectory.list[_index].offset = 0;
				pageDirectory.list[_index].length = 0;
			}

			pageDirectory.list[rid.slotNum+numberOfSlots].offset = (spaceToBeFilled==0)?-1:abs(pageDirectory.list[rid.slotNum+numberOfSlots].offset)+abs(pageDirectory.list[rid.slotNum+numberOfSlots].length)-abs(spaceToBeFilled);
			pageDirectory.list[rid.slotNum+numberOfSlots].length = abs(spaceToBeFilled);
			_pf->OpenFile(tableName.c_str(),tableHandle);
			insertDirectoryList(page,pageDirectory);
			tableHandle.WritePage(rid.pageNum,page);
			_pf->CloseFile(tableHandle);

		}
		else
		{
			RID referencedRid;
			insertTuple(tableName,data,referencedRid);
			_pf->OpenFile(tableName.c_str(),tableHandle);
			char referencedPage[PF_PAGE_SIZE];
			tableHandle.ReadPage(referencedRid.pageNum,&referencedPage);
			int _referencedRecordType = ReferencedRecord;
			SlotsDirectory _referencedDirectory ;
			getDirectoryList(&referencedPage,_referencedDirectory);


			writeToBuffer(&referencedPage,&_referencedRecordType,_referencedDirectory.list[referencedRid.slotNum].offset,0,size_int);
			tableHandle.WritePage(referencedRid.pageNum,&referencedPage);

			tableHandle.ReadPage(rid.pageNum,page);
			SlotsDirectory _newPageDirectory;
			getDirectoryList(page,_newPageDirectory);
			slot = pageDirectory.list[rid.slotNum];
			int _recordType = MutatedRecord;
			writeToBuffer(page,&_recordType,slot.offset,0,size_int);
			writeToBuffer(page,&referencedRid.pageNum,slot.offset+size_int,0,size_int);
			writeToBuffer(page,&referencedRid.slotNum,slot.offset+2*size_int,0,size_int);
			_newPageDirectory.list[rid.slotNum].length = 3*size_int;
			insertDirectoryList(page,_newPageDirectory);
			tableHandle.WritePage(rid.pageNum,page);
			_pf->CloseFile(tableHandle);
		}
	}

	return success;

}

RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
	if(_pf->fileExists(tableName.c_str()))
	{
		//open the file
		PF_FileHandle tableHandle;
		_pf->OpenFile(tableName.c_str(),tableHandle);
		//read the directory list and get the offset and length
		if(rid.pageNum>=tableHandle.GetNumberOfPages())
		{
			_pf->CloseFile(tableHandle);
			return failure;
		}
		else
		{
			char page[PF_PAGE_SIZE];
			SlotsDirectory pageDirectory;
			tableHandle.ReadPage(rid.pageNum,&page);
			_pf->CloseFile(tableHandle);
			getDirectoryList(&page,pageDirectory);

			if(pageDirectory.list.size()<rid.slotNum)
			{
				return failure;
			}
			else
			{
				SlotEntry slot;

				slot = pageDirectory.list[rid.slotNum]; // indexes start from 0
				// read the memory from offset to offset + length and return the value

				//if the record is deleted ..return failure

				if(slot.length<=0 && slot.offset <=0)
				{
					return failure;
				}


				int _tempOffset = slot.offset+size_int;// to skip the Datarecordtype
				writeToBuffer(data,&page,0,_tempOffset,slot.length-size_int);

				// check if natural record then  return data as is
				int _recordType = -2;
				writeToBuffer(&_recordType,&page,0,slot.offset,size_int);
				// if referred record
				// parse the slotNum and PageNum into a rid
				// call the ReadTuple on the new rid and return the data
				_recordType = (DataRecordType)_recordType;
				if(_recordType == NaturalRecord || _recordType == ReferencedRecord)
				{
					// do nothing . data shall be returned as is

				}
				else if(_recordType == MutatedRecord)
				{
					int _pageNum = -1, _slotNum = -1,_offset = 0 ;
					writeToBuffer(&_pageNum,data,0,_offset,size_int);
					_offset = _offset + size_int;
					writeToBuffer(&_slotNum,data,0,_offset,size_int);
					RID newRid;
					newRid.pageNum = _pageNum;
					newRid.slotNum = _slotNum;
					readTuple(tableName,newRid,data);
				}
				return success;
			}
		}
	}
	return failure;
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}

	char record[bufsize];
	readTuple(tableName,rid,&record);
	// parse the record value to read a particular attribute
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	int attributeOffset=0;

	int size_attribute=0;
	for(unsigned i=0;i<attrs.size();i++)
	{
	  if(strcmp(attrs[i].name.c_str(),attributeName.c_str())!=0)
	  {   if(attrs[i].type==TypeInt)
		{
		  attributeOffset+=size_int;
		}
	   else if(attrs[i].type==TypeReal)
	  {
		attributeOffset += size_float;
	  }
	   else if(attrs[i].type==TypeVarChar)
	   {
		 int varCharLength = -1;
		 writeToBuffer(&varCharLength,&record,0,attributeOffset,size_int);
		 attributeOffset += size_int;
		 attributeOffset += varCharLength;
	  }
	   else
	   {

	   }

	  }
	  else{
		  int type=attrs[i].type;
		  int compareType=TypeInt;
		  if(type==compareType)
			{
			  size_attribute=size_int;
			  writeToBuffer(data,&record,0,attributeOffset,size_attribute);
			  break;

			}
		  else if(attrs[i].type==TypeReal)
			{
				size_attribute= size_float;
				writeToBuffer(data,&record,0,attributeOffset,size_attribute);
				break;

			}
		   else if(attrs[i].type==TypeVarChar)
			{
				int varCharLength = -1;
				writeToBuffer(&varCharLength,&record,0,attributeOffset,size_int);
				size_attribute=varCharLength+size_int;
				writeToBuffer(data,&record,0,attributeOffset,size_attribute);
				break;

			}
		  else
		  {

		  }
	}

	}

	return success;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}
	vector<int> dataPageList;
	getAllPagesInHeader(tableName,dataPageList);
	for(int index= 0 ; index<dataPageList.size();index++)
	{
	if(dataPageList[index]==pageNumber)
	{
	return failure;
	}
	else{
	continue;
	}
	}

	//open file
	PF_FileHandle tableHandle;
	_pf->OpenFile(tableName.c_str(),tableHandle);
	// read the page
	char page[PF_PAGE_SIZE];
	tableHandle.ReadPage(pageNumber,&page);
	//getdirectoryList
	SlotsDirectory pageDirectory;
	getDirectoryList(&page,pageDirectory);
	//read all tuples into a vector
	int offset = 0;
	SlotEntry slot;
	// Sanitary check
	for(unsigned i=0;i<pageDirectory.list.size();i++)
	{
		slot = pageDirectory.list[i];
		if(slot.offset <= 0 && slot.length<=0)
		{
			continue;
		}
		else
		{
			if(offset <= slot.offset)
			{
				offset = offset + slot.length;
			}
			else
			{
				// error ... slot numbers do not have records in sequential order on the page
			}
		}

	}
	// for each tuple write sequentially and update the corresponding slot with new offset .
	// Note: Length of the tuple will not change ... only the offset
	offset = 0;
	for(unsigned i=0;i<pageDirectory.list.size();i++)
	{
		slot = pageDirectory.list[i];
		if(slot.offset <= 0 && slot.length<=0)
		{
			continue;
		}
		else
		{
			char record[slot.length];
			writeToBuffer(&record,&page,0, slot.offset , slot.length);
			writeToBuffer(&page,&record,offset,0,slot.length);
			pageDirectory.list[i].offset = offset;
			offset = offset + slot.length;
		}
	}
	pageDirectory.freeSpaceOffset = offset;
	// write the directorylist back to page
	insertDirectoryList(&page,pageDirectory);
	//write the page back to disk
	tableHandle.WritePage(pageNumber,&page);
	_pf->CloseFile(tableHandle);
	return success;
}

// scan returns an iterator to allow the caller to go through the results one by one.
RC RM::scan(const string tableName,
    const string conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RM_ScanIterator &rm_ScanIterator)
{

	if(!_pf->fileExists(tableName.c_str()))
	{
		return failure;
	}

	// Go to header page...fetch all the data pages. and go to each datapage and check for slotEntry which has a positive slot offset.
	//prepare an RID and call  your getAttribute method..check the attribute value iwth comparision operators if satisifies add RID to vector.
	vector<int> dataPageNums;
	getAllPagesInHeader(tableName,dataPageNums);

	char page[PF_PAGE_SIZE];
	PF_FileHandle fileHandle;
	_pf->OpenFile(tableName.c_str(),fileHandle);
	vector<RID> getAllRids;
	vector<RID> getSelectedRids;

	//data in directory listing
	for(unsigned int i=0; i<dataPageNums.size();i++)
	{
		fileHandle.ReadPage(dataPageNums[i],&page);
		SlotsDirectory directory;
		getDirectoryList(&page,directory);


		for(unsigned int slotIndex=0;slotIndex<directory.list.size();slotIndex++)
		{
			//check that the record is not a referenced record or a deleted record.
			if(directory.list[slotIndex].offset>=0 && directory.list[slotIndex].length>0){
				RID newRid ;
				newRid.pageNum=dataPageNums[i];
				newRid.slotNum=slotIndex;
				int _recordType = -1;
				writeToBuffer(&_recordType,&page,0,directory.list[slotIndex].offset,size_int);
				//check if the record is natural/mutated and insert the corresponding rid .
				//For referenced record do not insert rid.
				_recordType = (DataRecordType)_recordType;
				if(_recordType==NaturalRecord || _recordType == MutatedRecord)
				{
					getAllRids.push_back(newRid);
				}
				else
				{
					//for referenced record already the record is considered during the fetching of the mutated record
				}
			}
		}
	}

	char data[1000];// this is record size..no guarantees, should verify this
	Attribute attr;
	//go to attribute table and identify what type of attribute it is
	if(compOp!=NO_OP)
	readAttributeType(tableName.c_str(),conditionAttribute.c_str(),attr);
//	cout<<"size:"<<getAllRids.size()<<endl;
	for(unsigned int i=0;i<getAllRids.size();i++)
	{
	    // will get data in the attribute in data field
		if(compOp==NO_OP)
		{
			getSelectedRids.push_back(getAllRids[i]);
		}
		else
		{
			readAttribute(tableName.c_str(),getAllRids[i],conditionAttribute,&data);
			if(attr.type==TypeInt)
			{
			int expected = -1,given=-1;
			writeToBuffer(&expected,&data,0,0,size_int);
			if(compOp!=NO_OP)
			{
			writeToBuffer(&given,value,0,0,size_int);
			}
			switch(compOp){
			case EQ_OP: if(expected==given) {getSelectedRids.push_back(getAllRids[i]);}
			            break;
			case LT_OP: if(expected<given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case GT_OP: if(expected>given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case LE_OP: if(expected<=given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case GE_OP: if(expected>=given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case NE_OP: if(expected!=given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
//			case NO_OP: getSelectedRids.push_back(getAllRids[i]);
//						break;
			}
		}
		else if(attr.type==TypeReal)
		{
			float expected = -1,given=-1;
			writeToBuffer(&expected,&data,0,0,size_float);
			if(compOp!=NO_OP)
			{
			writeToBuffer(&given,value,0,0,size_float);
			}
			switch(compOp){
			case EQ_OP: if(expected==given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case LT_OP: if(expected<given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case GT_OP: if(expected>given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case LE_OP: if(expected<=given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case GE_OP: if(expected>=given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
			case NE_OP: if(expected!=given) {getSelectedRids.push_back(getAllRids[i]);}
						break;
//			case NO_OP: getSelectedRids.push_back(getAllRids[i]);
//						break;
			}

		}
		else if(attr.type==TypeVarChar)
		{
		string expected = "",given="";
		int _expectedLength=-1,_givenLength = -1;
		writeToBuffer(&_expectedLength,&data,0,0,size_int);
		char _expected[_expectedLength+1];
		writeToBuffer(&_expected,&data,0,size_int,_expectedLength);
		*((char *)&_expected+_expectedLength)='\0';
		expected = string((char *)&_expected);
//		expected=string((char *)data+size_int);

		if(compOp!=NO_OP)
		{
			writeToBuffer(&_givenLength,value,0,0,size_int);
			char _given[_givenLength+1];
			writeToBuffer(&_given,value,0,size_int,_givenLength);
			*((char *)&_given+_givenLength)='\0';
			given = string((char *)&_given);
//			given=string((char *)value);
		}
		switch(compOp){
			case EQ_OP:
				if(strcmp(expected.c_str(),given.c_str())==0)
			            {
						getSelectedRids.push_back(getAllRids[i]);}
			            break;
			case LT_OP:
				if(strcmp( expected.c_str(),given.c_str())<0)
            			{getSelectedRids.push_back(getAllRids[i]);}
            			break;
			case GT_OP:
				if(strcmp( expected.c_str(),given.c_str())>0)
            			{getSelectedRids.push_back(getAllRids[i]);}
            			break;
			case LE_OP:
				if(strcmp( expected.c_str(),given.c_str())<0||strcmp( expected.c_str(),given.c_str())==0)
						{getSelectedRids.push_back(getAllRids[i]);}
						break;
			case GE_OP:
				if(strcmp( expected.c_str(),given.c_str())>0||strcmp( expected.c_str(),given.c_str())==0)
						{getSelectedRids.push_back(getAllRids[i]);}
						break;
			case NE_OP:
				if(strcmp( expected.c_str(),given.c_str())!=0)
			            {getSelectedRids.push_back(getAllRids[i]);}
			            break;
//			case NO_OP:
//				getSelectedRids.push_back(getAllRids[i]);
//						break;
			}

		}
        //Now you have all the attributes that satisfy the where condition
		}
	}
//    cout<<getSelectedRids.size()<<endl;
	rm_ScanIterator.ridList=getSelectedRids;
	rm_ScanIterator.attrNames=attributeNames;
	rm_ScanIterator.tableName=tableName;
	_pf->CloseFile(fileHandle);
	return success;
}



RC RM::initialSetup()
{
	//cout<<"Initial Setup"<<endl;
	//create the catalog and attribute file if either of the two doesn't exist
	if(!(_pf->fileExists(_catalog) && _pf->fileExists(_attribute)))
	{
		//cout<<"entered if"<<endl;
		if(_pf->fileExists(_catalog))
		_pf->DestroyFile(_catalog);
		if(_pf->fileExists(_attribute))
		_pf->DestroyFile(_attribute);
		//creating and populating the Catalog file
		createCatalog();
		//creating and populating the Attribute file
		createAttribute();
	}
	//cout<<"Initial Setup end"<<endl;
	return success;
}
RC RM::createCatalog()
{
	try
	{
		int tuplesize,offset = 0;
		void *tuple;
		SlotEntry catalogEntry;
	// create catalog file
		_pf->CreateFile(_catalog);
	// open the catalog
		_pf->OpenFile(_catalog,fileHandle);
	// make header page
		char data[PF_PAGE_SIZE];
		headerPage(&data);
		int next_table_id = 3; // table_id starts at 1
		writeToBuffer(&data,&next_table_id,PF_PAGE_SIZE-2*size_int,0,size_int);
		fileHandle.AppendPage(&data);
		//free(data);
	// make first data page
		char data1[PF_PAGE_SIZE];
		dataPage(&data1);
		fileHandle.AppendPage(&data1);
		//free(data2);
	// add initial entries of catalog file
	    char data3[PF_PAGE_SIZE];
		fileHandle.ReadPage(1,&data3);
		SlotsDirectory directory;
		getDirectoryList(&data3,directory);

		//record catalog
		char tuple1[bufsize];
		prepareCatalogTuple(1,_catalog,1.0,&tuple1,&tuplesize);
		writeToBuffer(&data3,&tuple1,offset,0,tuplesize);
		catalogEntry.offset = offset;
		catalogEntry.length = tuplesize;
		offset = offset + tuplesize;
		directory.list.push_back(catalogEntry);
		//free(tuple);
		//record attribute
		char tuple2[bufsize];
		tuplesize=0;
		prepareCatalogTuple(2,_attribute,1.0,&tuple2,&tuplesize);
		writeToBuffer(&data3,&tuple2,offset,0,tuplesize);
		SlotEntry attributeEntry;
		attributeEntry.offset = offset;
		attributeEntry.length = tuplesize;
		offset = offset + tuplesize;
		directory.list.push_back(attributeEntry);
		//free(tuple);
		directory.freeSpaceOffset = offset;
		directory.length = directory.length+2*size_slot;
		insertDirectoryList(&data3,directory);
		fileHandle.WritePage(1,&data3);
		//free(data3);
	// close the catalog
		_pf->CloseFile(fileHandle);

	}catch(exception &e)
	{
		//cout<<e.what()<<endl;
	}
	return success;

}

RC RM::createAttribute()
{
	int tuplesize,offset = 0;
	char data[PF_PAGE_SIZE];
// create Attribute file
	_pf->CreateFile(_attribute);
// open the Attribute
	_pf->OpenFile(_attribute,fileHandle);
	//cout<<"point"<<endl;
// make header page

	headerPage(&data);
	fileHandle.AppendPage(&data);
	//free(data);
	//cout<<"point"<<endl;
// make first data page
	char data1[PF_PAGE_SIZE];
	dataPage(&data1);
	fileHandle.AppendPage(&data1);
	//free(data);
	//cout<<"point"<<endl;
// add entries of Attribute file
	fileHandle.ReadPage(1,&data1);
	SlotsDirectory directory;
	getDirectoryList(&data1,directory);

	vector<Attribute> cAttrs;
	tableStructure(0,cAttrs);
	for(unsigned i =0;i<cAttrs.size();i++)
	{
		char tuple[bufsize];
		prepareAttributeTuple(1,cAttrs[i].name,cAttrs[i].type,cAttrs[i].length,&tuple,&tuplesize);
		writeToBuffer(&data1,&tuple,offset,0,tuplesize);
		SlotEntry _slot;
		_slot.offset = offset;
		_slot.length = tuplesize;
		offset = offset + tuplesize;
		directory.list.push_back(_slot);
		//free(tuple);
		tuplesize = 0;
	}
	//cout<<"point"<<endl;
	vector<Attribute> aAttrs;
	tableStructure(1,aAttrs);
	int index_offset = 0;
	for(unsigned i =0;i<aAttrs.size();i++)
	{
		char tuple[1000];
		prepareAttributeTuple(2,aAttrs[i].name,aAttrs[i].type,aAttrs[i].length,&tuple,&tuplesize);
		writeToBuffer(&data1,&tuple,offset,0,tuplesize);
		SlotEntry _slot;
		_slot.offset = offset;
		_slot.length = tuplesize;
		offset = offset + tuplesize;
		directory.list.push_back(_slot);
		//free(tuple);
		tuplesize = 0;
	}
	index_offset = cAttrs.size() + aAttrs.size();
	directory.freeSpaceOffset = offset;
	directory.length = directory.length+index_offset*size_slot;
	insertDirectoryList(&data1,directory);
	fileHandle.WritePage(1,&data1);
	//printDirectory(directory);
	//free(data);
	//cout<<"point"<<endl;
// Write the next attr id in header page
   char	data2[PF_PAGE_SIZE];
	fileHandle.ReadPage(0,&data2);
	writeToBuffer(&data2,&index_offset,PF_PAGE_SIZE-2*size_int,0,size_int);
	//free(data);
	_pf->CloseFile(fileHandle);
	//cout<<"point"<<endl;
// close the Attribute file

	//cout<<"close"<<endl;
	return success;
}

RC RM::tableStructure(int type,vector<Attribute> &attrs)
{
	Attribute attr;
	switch(type)
	{
	case 0:
		//catalog table

		attr.name = "table_id";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		attrs.push_back(attr);

		attr.name = "tableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength)25;
		attrs.push_back(attr);

		attr.name = "table_version";
		attr.type = TypeReal;
		attr.length = (AttrLength)4;
		attrs.push_back(attr);

		break;
	case 1:
		attr.name = "table_id";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		attrs.push_back(attr);

		attr.name = "ColumnName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength)25;
		attrs.push_back(attr);

		attr.name = "ColumnType";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		attrs.push_back(attr);

		attr.name = "ColumnLength";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		attrs.push_back(attr);
		break;
//		attr.name = "Nullable";
//		attr.type = TypeInt;
//		attr.length = (AttrLength)4;
//		attrs.push_back(attr);

	default:
		break;
	}
	return success;
}

RC RM::headerPage(const void* buffer)
{
	// As per the original design last 4 bytes or sizeof(int)
	// will contain the number of entries of data pages in header page
	// At the creation of the file only 1 data page shall exist
	int numberOfPages = 1;
	int offset = 0;
	// number of available data pages is set to 1
	writeToBuffer(buffer,&numberOfPages,offset,0,size_int);
	// offset to the first data page is 1 as page indexes start from 0
	offset = offset + size_int;
	writeToBuffer(buffer,&numberOfPages,offset,0,size_int);
	// write 0 at the end of header page to indicate header page is not full.
	// here 0 will be replaced by offset or page number of overflow header page
	int value = 0;
	offset = PF_PAGE_SIZE-size_int;
	writeToBuffer(buffer,&value,offset,0,size_int);
	return success;
}

RC RM::dataPage(const void* data)
{
	// Data page format
	// Data page will contain records from offset 0 .Last x bytes of the page will contain
	// a Directory of the slots
	// Format of Directory
	// <Slot n><Slot n-1>......<Slot 1><pointer-to-//free-space><length-of-directory>
	// Note : Slot 1 is towards the right end and Slot n is towards the left end
	SlotsDirectory directory;
	directory.freeSpaceOffset=0;
	directory.length = slotControlBuffers*size_int;
	insertDirectoryList(data,directory);
	return success;
}

RC RM::getDirectoryList(const void *data,SlotsDirectory &directory)
{
	int offset=PF_PAGE_SIZE;

	offset = offset - size_int; // Directory length
	writeToBuffer(&directory.length,data,0,offset,size_int);
	offset = offset - size_int; // //free space offset
	writeToBuffer(&directory.freeSpaceOffset,data,0,offset,size_int);

	int numberOfSlots = (directory.length-slotControlBuffers*size_int)/size_slot;
	for(int slotIndex=1;slotIndex<=numberOfSlots;slotIndex++)
	{
		offset = offset - size_slot;
		SlotEntry entry ;
		entry.offset = -1;
		entry.length = -1;
		writeToBuffer(&entry.offset,data,0,offset,size_int);
		writeToBuffer(&entry.length,data,0,offset+size_int,size_int);
		directory.list.push_back(entry);
	}
	return success;
}

RC RM::insertDirectoryList(const void* data,SlotsDirectory directory)
{
	int offset=PF_PAGE_SIZE;
	int numberOfSlots = directory.list.size();
	directory.length = numberOfSlots*size_slot + slotControlBuffers*size_int;

	//To store the length of directory
	offset = offset - size_int;
	writeToBuffer(data,&directory.length,offset,0,size_int);
	//To store the //free space offset
	offset = offset - size_int;
	writeToBuffer(data,&directory.freeSpaceOffset,offset,0,size_int);

	for(int slotIndex=1;slotIndex<=numberOfSlots;slotIndex++)
	{
		offset = offset - size_slot;
		writeToBuffer(data,&directory.list[slotIndex-1].offset,offset,0,size_int);
		writeToBuffer(data,&directory.list[slotIndex-1].length,offset+size_int,0,size_int);
	}
	return success;
}

RC RM::prepareCatalogTuple(int table_id,string tablename,float version,const void *data,int *tupleSize)
{
	int offset=0;
	//Adding a parity bit RecordType
	int _recordType = NaturalRecord;
	writeToBuffer(data,&_recordType,offset,0,size_int);
	offset = offset + size_int;
	int tableNameSize = tablename.length();
	writeToBuffer(data,&table_id,offset,0,size_int);
	offset = offset+size_int;
	writeToBuffer(data,&tableNameSize,offset,0,size_int);
	offset = offset + size_int;
	writeToBuffer(data,tablename.c_str(),offset,0,tableNameSize);
	offset = offset + tableNameSize;
	writeToBuffer(data,&version,offset,0,size_float);
	offset = offset + size_float;
	*tupleSize = offset;
	return success;
}

RC RM::prepareAttributeTuple(int table_id,string columnName,int columnType,int length,void *data,int *tupleSize)
{

	int offset = 0;
	int columnNameSize = columnName.length();
	int _recordType = NaturalRecord;
	writeToBuffer(data,&_recordType,offset,0,size_int);
	offset = offset + size_int;
	writeToBuffer(data,&table_id,offset,0,size_int);
	offset = offset + size_int;
	writeToBuffer(data,&columnNameSize,offset,0,size_int);
	offset = offset + size_int;
	writeToBuffer(data,columnName.c_str(),offset,0,columnNameSize);
	offset = offset + columnNameSize;
	writeToBuffer(data,&columnType,offset,0,size_int);
	offset = offset + size_int;
	writeToBuffer(data,&length,offset,0,size_int);
	offset = offset + size_int;
	*tupleSize = offset;
	return success;
}
RC RM::writeToBuffer(const void* buffer,const void* data, int offset1,int offset2,int length)
{
	memcpy((char *)buffer+offset1,(char *)data+offset2,length);
	return success;
}

RC RM::getRecordLength(const string tableName,int *recordLength,const void *data)
{

	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	int recordSize=0;
	for(unsigned i=0;i<attrs.size();i++)
	{
		if(attrs[i].type==TypeInt)
		{
			recordSize += size_int;
		}
		else if(attrs[i].type==TypeReal)
		{
			recordSize += size_float;
		}
		else if(attrs[i].type==TypeVarChar)
		{
			int varCharLength = -1;
			writeToBuffer(&varCharLength,data,0,recordSize,size_int);
			recordSize += size_int;
			recordSize += varCharLength;
		}
		else
		{
			// Shouldn't be here

		}
	}

	*recordLength = recordSize;

	return success;
}


RC RM::printDirectory(SlotsDirectory directory)
{
	cout<<endl<<"Directory free space "<<directory.freeSpaceOffset<<endl;
	cout<<"Directory length"<<directory.length<<endl;
	cout<<"Directory # slots" << directory.list.size()<<endl;
	for(unsigned i=0;i<directory.list.size();i++)
	{
		cout<<"Index "<<i+1 << " Offset "<<directory.list[i].offset<<" Length" <<directory.list[i].length<<endl;
	}
	return success;
}

RC RM::freeSpaceInTable(const string tableName,int length,int *page,int *offset)
{
	//open file
	vector<int> pages;// all the page numbers
	getAllPagesInHeader(tableName.c_str(),pages);


	PF_FileHandle tableHandle;
	_pf->OpenFile(tableName.c_str(),tableHandle);
	// read the header page and gather the list of page numbers in vector
	// scan through every page for free space using freeSpaceInPage function
	char data[PF_PAGE_SIZE];
	int pageNumber = pageNotFound; // currently pointing to -1
	for(unsigned i=0;i<pages.size();i++)
	{
		tableHandle.ReadPage(pages[i],&data);
		int freeSpace = -1;
		freeSpaceInPage(&data,length,&freeSpace);
		if(freeSpace>=0)
		{
			*offset = freeSpace;
			pageNumber = pages[i];
			break;
		}
	}
	// return the page number of the first page with free space else return pageNotFound
	*page = pageNumber;
	//close file
	_pf->CloseFile(tableHandle);
	return success;
}
RC RM::freeSpaceInPage(void *page,int tupleLength,int *offset)
{
	SlotsDirectory pageDirectory;
	getDirectoryList(page,pageDirectory);
	int numberOfSlots = pageDirectory.list.size();
	if(numberOfSlots==0)
	{
		*offset = pageDirectory.freeSpaceOffset; //which should be zero
		return success;
	}
	// basically checking for long empty spaces between the slots
	int continuousFreeSpace = 0 ,startSlot = -1,startOffset = 0;
	SlotEntry currentSlot;
	for(int i=0;i<numberOfSlots;i++)
	{
		currentSlot = pageDirectory.list[i];
		if(currentSlot.offset<=0 && currentSlot.length<=0)
		{
			if(startSlot == -1)
			{
				startSlot = i;
				// As the when a tuple is deleted the offset say from 250 is changed to -250
				startOffset = (-1)*currentSlot.offset;
			}

			continuousFreeSpace = continuousFreeSpace + (-1)*currentSlot.length;
			if(continuousFreeSpace >= tupleLength)
			{
				*offset = startOffset;
				return success;
			}
		}
		else
		{
			startSlot = -1;
		}
	}

	// if none of the previously filled slots are empty
	// check in the remaining free space memory towards the end of page
	int freeSpaceAtEndOfPage = (PF_PAGE_SIZE - pageDirectory.freeSpaceOffset) - pageDirectory.length-size_slot;
	if(startSlot!=-1 && (continuousFreeSpace+freeSpaceAtEndOfPage)>=tupleLength)
	{
		// nothing to set.. offset is already set
		return success;
	}
	else if(freeSpaceAtEndOfPage >= tupleLength)
	{
		*offset = pageDirectory.freeSpaceOffset;
		return success;
	}
	*offset = -1;
	return success;
}


RC RM::getAllPagesInHeader(const string tableName,vector<int> &pages)
{
	int nextHeaderPage=0;
	//open the file
	PF_FileHandle tableHandle;
	_pf-> OpenFile(tableName.c_str(),tableHandle);
	// read the 0 page <header page>
	char page[PF_PAGE_SIZE];
	do{
	tableHandle.ReadPage(nextHeaderPage,&page);
	// read the first 4 bytes and get the number of pages
	int numberOfPages = 0;
	writeToBuffer(&numberOfPages,&page,0,0,size_int);
	int pageNumber = -1, offset = size_int;
	// iterate through each 4 bytes forward and store the pages in vector
	for(int i=0;i<numberOfPages;i++)
	{
		writeToBuffer(&pageNumber,&page,0,offset,size_int);
		offset = offset+size_int;
		pages.push_back(pageNumber);
	}
	writeToBuffer(&nextHeaderPage,&page,0,PF_PAGE_SIZE-size_int,size_int);
	}while(nextHeaderPage!=0);
	//close the file
	_pf->CloseFile(tableHandle);
	return success;
}
RM_ScanIterator::RM_ScanIterator() {
	RM *rm=RM::Instance();
	rm->getAttributes(tableName,_attrs);
	currentIndex=-1;
}
RM_ScanIterator::~RM_ScanIterator() {}

RC RM_ScanIterator::readAttributeType(const string tableName, const string attributeName,Attribute &attr)
{
// given tableName and attribute Name you should return attribute type for it
// compare attributeName with vector of attributes I fetched
for(unsigned i=0;i<_attrs.size();i++)
{
  if(strcmp(_attrs[i].name.c_str(),attributeName.c_str())==0)
  {
	  attr.type=_attrs[i].type;
	  attr.length = _attrs[i].length;
	 break;
  }
}

  return success;
}

// "data" follows the same format as RM::insertTuple()
RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    RM *rm=RM::Instance();
    currentIndex+=1;
	if(currentIndex<ridList.size())
   {
     Attribute attr;
	 rid=ridList[currentIndex];
	 int offset=0;
	 for(unsigned int index=0;index<attrNames.size();index++)
	 {

		rm->readAttributeType(tableName,attrNames[index],attr);
		char attributeData[1000];
		rm->readAttribute(tableName,rid,attrNames[index],&attributeData);
		if(attr.type==TypeInt)
		{
		rm->writeToBuffer(data,&attributeData,offset,0,size_int);
		offset+=size_int;
		}
		else if(attr.type==TypeReal)
		{
			rm->writeToBuffer(data,&attributeData,offset,0,size_float);
			offset+=size_float;
		}
		else if(attr.type==TypeVarChar)
		{
//			string value=string((char *)attributeData);
			int dataLength=0;
			rm->writeToBuffer(&dataLength,&attributeData,0,0,size_int);
			dataLength+=size_int;
			rm->writeToBuffer(data,&attributeData,offset,0,dataLength);
			offset+=dataLength;
//			*((char *)data+offset+dataLength) = '\0';
//            offset=offset+dataLength;

		}
		else
		{
			//do nothing
		}

     }

	 return success;
   }
   else{
	   return RM_EOF;
   }

 }
RC RM_ScanIterator::close() {
	currentIndex=ridList.size();
	return -1; }

RC RM::readAttributeType(const string tableName, const string attributeName,Attribute &attr)
{
// given tableName and attribute Name you should return attribute type for it
// compare attributeName with vector of attributes I fetched
vector<Attribute> attrs;

getAttributes(tableName,attrs);

for(unsigned i=0;i<attrs.size();i++)
{
  if(strcmp(attrs[i].name.c_str(),attributeName.c_str())==0)
  {

	  attr.type=attrs[i].type;
	  attr.length = attrs[i].length;
	 break;
  }
}

  return success;
}


RC RM::reorganizeTable(const string tableName)
{

if(!_pf->fileExists(tableName.c_str()))
{
return failure;
}
vector<int> dataPages;
getAllPagesInHeader(tableName,dataPages);
int dataPagesSize = (int)dataPages.size();
char page[PF_PAGE_SIZE];
PF_FileHandle tableHandle;

for(int pageIndex = 0 ;pageIndex<dataPagesSize;pageIndex++)
{
reorganizePage(tableName,dataPages[pageIndex]);
_pf->OpenFile(tableName.c_str(),tableHandle);
tableHandle.ReadPage(dataPages[pageIndex],&page);
SlotsDirectory slotList ;
getDirectoryList(&page,slotList);
SlotsDirectory newDir;
int freeSpaceOffset = 0;
for(int slotIndex = 0 ; slotIndex<slotList.list.size();slotIndex++)
{
//Need to rearrange them
SlotEntry slot = slotList.list[slotIndex];
if(slot.length>0 && slot.offset>=0)
{
freeSpaceOffset+=slot.length;
newDir.list.push_back(slot);
continue;
}
else
{

}
}
newDir.freeSpaceOffset = freeSpaceOffset;
newDir.length = slotControlBuffers*size_int+newDir.list.size()*size_slot;
insertDirectoryList(&page,newDir);
tableHandle.WritePage(dataPages[pageIndex],&page);
_pf->CloseFile(tableHandle);
}

int offset = 0, insertPageIndex = 0;

char page1[PF_PAGE_SIZE];
SlotsDirectory insertDirectory;
char insertPage[PF_PAGE_SIZE];
tableHandle.ReadPage(dataPages[insertPageIndex],&insertPage);
getDirectoryList(&insertPage,insertDirectory);
offset = insertDirectory.freeSpaceOffset;
int freeSpaceRemaining = offset - insertDirectory.length;

for(int pageIndex = 1 ; pageIndex<dataPagesSize ; pageIndex++)
{
// For each page pass through all the records ..
SlotsDirectory currentPageDirectory;
tableHandle.ReadPage(dataPages[pageIndex],&page1);
getDirectoryList(&page1,currentPageDirectory);
for(int slotIndex = 0;slotIndex<currentPageDirectory.list.size();slotIndex++)
{
SlotEntry slot = currentPageDirectory.list[slotIndex];
if(freeSpaceRemaining>=(slot.length+size_slot))
{
writeToBuffer(&insertPage,&page1,offset,slot.offset,slot.length);
SlotEntry newSlot;
newSlot.length = slot.length;
newSlot.offset = offset;
insertDirectory.list.push_back(newSlot);
offset = offset + slot.length;
freeSpaceRemaining -= slot.length;
currentPageDirectory.list[slotIndex].offset=-1;
currentPageDirectory.list[slotIndex].length=-1;
}
else
{
//No more free space in current page .. move on to the next page
insertDirectoryList(&insertPage,insertDirectory);
tableHandle.WritePage(dataPages[insertPageIndex],&insertPage);
insertPageIndex++;

if(insertPageIndex == pageIndex)
{
//write the page back to file .
SlotsDirectory _tempDir;
int _tempOffset = 0;
for(int i=0;i<currentPageDirectory.list.size();i++)
{
SlotEntry _slot = currentPageDirectory.list[i];
if(_slot.length>0 && _slot.offset >=0)
{
_tempDir.list.push_back(_slot);
_tempOffset += _slot.length;
}
}
_tempDir.freeSpaceOffset = _tempOffset;

insertDirectoryList(&page1,currentPageDirectory);
tableHandle.WritePage(dataPages[pageIndex],&page1);
_pf->CloseFile(tableHandle);
reorganizePage(tableName,dataPages[pageIndex]);
_pf->OpenFile(tableName.c_str(),tableHandle);
tableHandle.ReadPage(dataPages[pageIndex],&insertPage);
insertDirectory = _tempDir;
break;
}
else
{
tableHandle.ReadPage(dataPages[insertPageIndex],&insertPage);
SlotsDirectory newPagedir;
getDirectoryList(&insertPage,newPagedir);
insertDirectory = newPagedir;
slotIndex--;
}
offset = insertDirectory.freeSpaceOffset;
freeSpaceRemaining = offset - insertDirectory.length;
}
}

}

_pf->CloseFile(tableHandle);

return success;
}



