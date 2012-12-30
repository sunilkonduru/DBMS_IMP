
# include "qe.h"
#include <map>
#include <vector>

const int size_int = sizeof(int);
const int size_float = sizeof(float);
const int qe_success = 0;
const int qe_failure = -1;
const int bufsize = 200;
const int parityHashKey = 3;


Filter::Filter(Iterator *input,                         // Iterator of input R
               const Condition &condition               // Selection condition
				)
	{
		_input = input;
		_condition = condition;
//		_rm = RM::Instance();
		_input->getAttributes(_attrs);
	}

Filter::~Filter()
	{

	}

RC preprocess()
{

}

RC Filter::getNextTuple(void *data) {

//	CompOp _op = _condition.op;
	Value conditionValue = _condition.rhsValue;
	// 1. Get the next tuple from iterator
	vector<Attribute> attrs;
	_input->getAttributes(attrs);
	RC rc = _input->getNextTuple(data);
	while(rc!=QE_EOF)
	{
		if(_condition.op == NO_OP) return qe_success;

		// 2. If condition is on value
		if(_condition.bRhsIsAttr==false)
		{
			// 2.a read the attribute and then fetch the attribute value
			int offset = 0;
			for(unsigned int i =0 ; i<attrs.size();i++)
			{
				if(attrs[i].name==_condition.lhsAttr)
				{
					if(attrs[i].type==TypeInt)
					{
//						value.data = malloc(size_int);
						int Cval=0,Gval;
						_helper.writeToBuffer(&Gval,data,0,offset,size_int);
						_helper.writeToBuffer(&Cval,conditionValue.data,0,0,size_int);
						if(_condition.op == EQ_OP &&
							Gval == Cval)
						{
							return qe_success;
						}
						else if(_condition.op == LE_OP &&
								Gval <= Cval)
						{
							return qe_success;
						}
						else if(_condition.op == LT_OP &&
								Gval < Cval)
						{
							return qe_success;
						}
						else if(_condition.op == GE_OP &&
								Gval >= Cval)
						{
							return qe_success;
						}
						else if(_condition.op == GT_OP &&
								Gval > Cval)
						{
							return qe_success;
						}
						else if(_condition.op == NE_OP &&
								Gval != Cval)
						{
							return qe_success;
						}
						else
						{
//							return qe_failure;
						}
					}
					else if(attrs[i].type == TypeReal)
					{
						float Cval=0,Gval=0;
						_helper.writeToBuffer(&Gval,data,0,offset,size_float);
						_helper.writeToBuffer(&Cval,conditionValue.data,0,0,size_float);
						if(_condition.op == EQ_OP &&
							Gval == Cval)
						{
							return qe_success;
						}
						else if(_condition.op == LE_OP &&
								Gval <= Cval)
						{
							return qe_success;
						}
						else if(_condition.op == LT_OP &&
								Gval < Cval)
						{
							return qe_success;
						}
						else if(_condition.op == GE_OP &&
								Gval >= Cval)
						{
							return qe_success;
						}
						else if(_condition.op == GT_OP &&
								Gval > Cval)
						{
							return qe_success;
						}
						else if(_condition.op == NE_OP &&
								Gval != Cval)
						{
							return qe_success;
						}
						else
						{
//							return qe_failure;
						}
					}
					else if(attrs[i].type == TypeVarChar)
					{
						int GLength = 0,CLength = 0;
						_helper.writeToBuffer(&GLength,data,0,offset,size_int);
						_helper.writeToBuffer(&CLength,conditionValue.data,0,0,size_int);
						offset+=size_int;
						char c[bufsize],g[bufsize];
						int gLen=GLength;
						int cLen=CLength;
						_helper.writeToBuffer(&g,data,0,offset,gLen);
						_helper.writeToBuffer(&c,conditionValue.data,0,size_int,cLen);
						g[gLen]='\0';
						c[cLen]='\0';
						string Cval(c),Gval(g);
						offset+= gLen;
						if(_condition.op == EQ_OP &&
							Gval == Cval)
						{
							return qe_success;
						}
						else if(_condition.op == LE_OP &&
								Gval <= Cval)
						{
							return qe_success;
						}
						else if(_condition.op == LT_OP &&
								Gval < Cval)
						{
							return qe_success;
						}
						else if(_condition.op == GE_OP &&
								Gval >= Cval)
						{
							return qe_success;
						}
						else if(_condition.op == GT_OP &&
								Gval > Cval)
						{
							return qe_success;
						}
						else if(_condition.op == NE_OP &&
								Gval != Cval)
						{
							return qe_success;
						}
						else
						{
//							return qe_failure;
						}
					}
					break;
				}
				else if(attrs[i].type == TypeInt || attrs[i].type == TypeReal)
				{
					offset += size_int;
				}
				else if(attrs[i].type==TypeVarChar)
				{
					int stringLength = 0;
					_helper.writeToBuffer(&stringLength,data,0,offset,size_int);
					int strleng=stringLength;
					offset += size_int;
					offset += strleng;
				}
			}

		}//end of if isRHSAttribute
		else if(_condition.bRhsIsAttr==true)
		{
			// 3. If condition is on attribute
		}
		rc = _input->getNextTuple(data);
	}

	return QE_EOF;
}
// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs = _attrs;
}



Project::Project(Iterator *input,                            // Iterator of input R
                const vector<string> &attrNames)           // vector containing attribute names
{
	 _input=input;
	 _attrNames=attrNames;
//	 _rm=RM::Instance();
	 initializeGetAttributes();
}

Project::~Project()
{

}

RC Project::getNextTuple(void *data) {
	//find out the attributes
	vector<Attribute> attrs;
	char tuple[bufsize];
	RC rc=_input->getNextTuple(&tuple);
	if(rc==qe_failure) return QE_EOF;
	int attributeLocation=0;
	int destinationAddress=0;
	int sizeOfString=0;
	int tempsize=0;
	//get the attributes and fill the data
	_input->getAttributes(attrs);
	for(int i=0;i<_attrNames.size();i++)
	{
		attributeLocation = 0;
		for(int j=0;j<attrs.size();j++)
		{
			if(strcmp(_attrNames[i].c_str(),attrs[j].name.c_str())!=0)
			{
				if(attrs[j].type==TypeInt||attrs[j].type==TypeReal)
					attributeLocation+=attrs[j].length;
				else if(attrs[j].type==TypeVarChar)
				{
					_helper.writeToBuffer(&sizeOfString,&tuple,0,attributeLocation,size_int);
					attributeLocation+=size_int;
					attributeLocation+=sizeOfString;
				}
			}
			else if(strcmp(_attrNames[i].c_str(),attrs[j].name.c_str())==0)
			{
				if(attrs[j].type==TypeInt||attrs[j].type==TypeReal)
				{
					_helper.writeToBuffer(data,&tuple,destinationAddress,attributeLocation,attrs[j].length);
					destinationAddress+=attrs[j].length;
					attributeLocation+=attrs[j].length;
				}
				else if(attrs[j].type==TypeVarChar)
				{
					_helper.writeToBuffer(&sizeOfString,&tuple,0,attributeLocation,size_int);
					tempsize=size_int+sizeOfString;
					_helper.writeToBuffer(data,&tuple,destinationAddress,attributeLocation,tempsize);
					destinationAddress+=tempsize;
					attributeLocation+=tempsize;
				}
				break;
			}
		}
	}

	return qe_success;
}
// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs=_attributes;
}

void Project::initializeGetAttributes()
{
    vector<Attribute> attributes;
	Attribute attr;
    _input->getAttributes(attributes);
	  for(int i=0;i<_attrNames.size();i++)
	  {
		for(int j=0;j<attributes.size();j++)
		{
			if(strcmp(_attrNames[i].c_str(),attributes[j].name.c_str())==0)
			{
				 _attributes.push_back(attributes[j]);
				 break;
			}

		}
	  }
}

NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
	   TableScan *rightIn,                           // TableScan Iterator of input S
	   const Condition &condition,                   // Join condition
	   const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
		)
{
	 _leftIn=leftIn;
	 _rightIn=rightIn;
	 _condition=condition;
//	 _rm=RM::Instance();
	 _helper.populateAttribute(_leftIn,_rightIn,_attrs);

	 _LIndex=0;
	 _RIndex=0;

	 preprocess();
}

NLJoin::~NLJoin()
{

}

RC NLJoin::preprocess()
{
	char leftTuple[bufsize];
	char rightTuple[bufsize];
	vector<Attribute> attrs1;
	vector<Attribute> attrs2;
	_leftIn->getAttributes(attrs1);
	_rightIn->getAttributes(attrs2);

	while(_leftIn->getNextTuple(&leftTuple)!=QE_EOF)
	{
		Value value;
		int size=0;
		_helper.sizeOfTuple(&leftTuple,attrs1,size);
		value.data = malloc(size);
		_helper.writeToBuffer(value.data,&leftTuple,0,0,size);
		value.size=size;
		leftValues.push_back(value);
	}

	while(_rightIn->getNextTuple(&rightTuple)!=QE_EOF)
	{
		Value value;
		int size=0;
		_helper.sizeOfTuple(&rightTuple,attrs2,size);
		value.data = malloc(size);
		_helper.writeToBuffer(value.data,&rightTuple,0,0,size);
		value.size=size;
		rightValues.push_back(value);
	}
}

RC NLJoin::getNextTuple(void *data) {
	char leftTuple[bufsize];
	char rightTuple[bufsize];
	vector<Attribute> attrs1;
	vector<Attribute> attrs2;
	_leftIn->getAttributes(attrs1);
	_rightIn->getAttributes(attrs2);
	//first get the condition attribute location

	int sizeOfLeftTuple=0,sizeOfRightTuple=0;
	while(_LIndex<leftValues.size())
	{
		_helper.writeToBuffer(&leftTuple,leftValues[_LIndex].data,0,0,leftValues[_LIndex].size);
		while(_RIndex<rightValues.size())
		{
			_helper.writeToBuffer(&rightTuple,rightValues[_RIndex].data,0,0,rightValues[_RIndex].size);
			RC rc=_helper.CompareValue(&leftTuple,attrs1,&rightTuple,attrs2,_condition);
			//for joining stuff
			if(rc==qe_success)
			{
//				_helper.sizeOfTuple(leftTuple,attrs1,sizeOfLeftTuple);
				sizeOfLeftTuple = leftValues[_LIndex].size;
				_helper.writeToBuffer(data,&leftTuple,0,0,sizeOfLeftTuple);
//				_helper.sizeOfTuple(rightTuple,attrs2,sizeOfRightTuple);
				sizeOfRightTuple = rightValues[_RIndex].size;
				_helper.writeToBuffer(data,&rightTuple,sizeOfLeftTuple,0,sizeOfRightTuple);
				_RIndex++;
				return qe_success;
			}
			else
			{
				//check for other one
			}
			_RIndex++;
		}
		_RIndex=0;
		_LIndex++;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs=_attrs;
}


INLJoin::INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
        )
{
	_leftIn=leftIn;
	 _rightIn=rightIn;
	 _condition=condition;
//	 _rm=RM::Instance();
	 _helper.populateAttribute(_leftIn,_rightIn,_attrs);

	 _LIndex=0;
	 change=true;
	 preprocess();
}

INLJoin::~INLJoin()
{

}

RC INLJoin::preprocess()
{
	char leftTuple[bufsize];
	vector<Attribute> attrs1;
	vector<Attribute> attrs2;
	_leftIn->getAttributes(attrs1);
	_rightIn->getAttributes(attrs2);

	while(_leftIn->getNextTuple(&leftTuple)!=QE_EOF)
	{
		Value value;
		int size=0;
		_helper.sizeOfTuple(&leftTuple,attrs1,size);
		value.data = malloc(size);
		_helper.writeToBuffer(value.data,&leftTuple,0,0,size);
		value.size=size;
		leftValues.push_back(value);
	}
}

RC INLJoin::getNextTuple(void *data) {
	char leftTuple[bufsize];
	char rightTuple[bufsize];
	vector<Attribute> attrs1;
	vector<Attribute> attrs2;
	_leftIn->getAttributes(attrs1);
	_rightIn->getAttributes(attrs2);
	Attribute attr;
	Value leftValue;
	//first get the condition attribute location
	int sizeOfLeftTuple=0,sizeOfRightTuple=0;
	while(_LIndex<leftValues.size())
	{
		_helper.writeToBuffer(leftTuple,leftValues[_LIndex].data,0,0,leftValues[_LIndex].size);
		_helper.getValueInTuple(&leftTuple,attrs1,_condition.lhsAttr,attr.type,leftValue);
		if(change)
		{
			_rightIn->setIterator(_condition.op,leftValue.data);
			change=false;
		}
		while(_rightIn->getNextTuple(&rightTuple)!=QE_EOF)
		{
			RC rc=_helper.CompareValue(&leftTuple,attrs1,&rightTuple,attrs2,_condition);
			//for joining stuff
			if(rc==qe_success)
			{
//				_helper.sizeOfTuple(leftTuple,attrs1,sizeOfLeftTuple);
				sizeOfLeftTuple = leftValues[_LIndex].size;
				_helper.writeToBuffer(data,&leftTuple,0,0,sizeOfLeftTuple);
				_helper.sizeOfTuple(&rightTuple,attrs2,sizeOfRightTuple);
				_helper.writeToBuffer(data,&rightTuple,sizeOfLeftTuple,0,sizeOfRightTuple);
				return qe_success;
			}
			else
			{
				//check for other one
			}
		}
		_LIndex++;
		change=true;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs = _attrs;
}


Helper::Helper()
{
//  _rm=RM::Instance();
}


Helper::~Helper()
{

}

RC Helper::writeToBuffer(const void* buffer,const void* data, int offset1,int offset2,int length)
{
	memcpy((char *)buffer+offset1,(char *)data+offset2,length);
	return qe_success;
}


RC Helper::getValueInTuple(void *data,vector<Attribute> attrs,string attrName,AttrType &type,Value &value)
{
//	RM *_rm = RM::Instance();
	int offset = 0;
	for(unsigned int i =0 ; i<attrs.size();i++)
	{
		if(attrs[i].name==attrName)
		{
			type = attrs[i].type;
			//cout<<type<<endl;
			if(type==TypeInt)
			{
				//cout<<"Int"<<TypeInt<<endl;
			//	value.data = malloc(size_int);
				value.data = malloc(size_int);
				writeToBuffer(value.data,data,0,offset,size_int);
			}
			else if(type==TypeReal)
			{
				//cout<<"real"<<TypeReal<<endl;
				value.data = malloc(size_float);
				writeToBuffer(value.data,data,0,offset,size_float);
			}
			else if(type==TypeVarChar)
			{
				//cout<<"varchar"<<TypeVarChar<<endl;
				int stringLength = 0;
				writeToBuffer(&stringLength,data,0,offset,size_int);
				offset += size_int;
				value.data = malloc(stringLength+size_int);
				writeToBuffer(value.data,&stringLength,0,0,size_int);
				writeToBuffer(value.data,data,size_int,offset,stringLength);
			}
				break;
			}
			else if(attrs[i].type == TypeInt || attrs[i].type == TypeReal)
			{
				offset += size_int;
			}
			else
			{
				int stringLength = 0;
				writeToBuffer(&stringLength,data,0,offset,size_int);
				offset += size_int;
				offset += stringLength;
			}
	}

	return qe_success;
}



RC Helper::CompareValue(void *leftData,vector<Attribute> leftAttrs,void *rightData,vector<Attribute> rightAttrs,Condition condition)
{
Value leftValue,rightValue;
AttrType type;
getValueInTuple(leftData,leftAttrs,condition.lhsAttr,type,leftValue);
getValueInTuple(rightData,rightAttrs,condition.rhsAttr,type,rightValue);

if(type==TypeInt)
{
	int LVal=0,RVal=0;
	writeToBuffer(&LVal,leftValue.data,0,0,size_int);
	writeToBuffer(&RVal,rightValue.data,0,0,size_int);
  if(condition.op==EQ_OP&&LVal==RVal)
  {
  return qe_success;
  }
  else if(condition.op==LE_OP && LVal<=RVal)
  {
  return qe_success;
  }
  else if(condition.op==LT_OP && LVal<RVal)
  {
  return qe_success;
  }
  else if(condition.op==GE_OP && LVal>=RVal)
  {
  return qe_success;
  }

  else if(condition.op==GT_OP && LVal>RVal)
  {
  return qe_success;
  }
  else if(condition.op==NE_OP && LVal!=RVal)
  {
  return qe_success;
  }

}
else if(type==TypeReal)
{
float LVal=0,RVal=0;
writeToBuffer(&LVal,leftValue.data,0,0,size_float);
writeToBuffer(&RVal,rightValue.data,0,0,size_float);
  if(condition.op==EQ_OP&&LVal==RVal)
  {
  return qe_success;
  }
  else if(condition.op==LE_OP && LVal<=RVal)
  {
  return qe_success;
  }
  else if(condition.op==LT_OP && LVal<RVal)
  {
  return qe_success;
  }
  else if(condition.op==GE_OP && LVal>=RVal)
  {
  return qe_success;
  }

  else if(condition.op==GT_OP && LVal>RVal)
  {
  return qe_success;
  }
  else if(condition.op==NE_OP && LVal!=RVal)
  {
  return qe_success;
  }
}
else if(type==TypeVarChar)
{

int LSize=0,RSize=0;
writeToBuffer(&LSize,leftValue.data,0,0,size_int);
writeToBuffer(&RSize,rightValue.data,0,0,size_int);
char l[bufsize], r[bufsize];
int lsize=LSize;
int rsize=RSize;
writeToBuffer(&l,leftValue.data,0,size_int,lsize);
writeToBuffer(&r,rightValue.data,0,size_int,rsize);
l[lsize]='\0';
r[rsize]='\0';
string LVal(l),RVal(r);
  if(condition.op==EQ_OP&&LVal==RVal)
  {
  return qe_success;
  }
  else if(condition.op==LE_OP && LVal<=RVal)
  {
  return qe_success;
  }
  else if(condition.op==LT_OP && LVal<RVal)
  {
  return qe_success;
  }
  else if(condition.op==GE_OP && LVal>=RVal)
  {
  return qe_success;
  }

  else if(condition.op==GT_OP && LVal>RVal)
  {
  return qe_success;
  }
  else if(condition.op==NE_OP && LVal!=RVal)
  {
  return qe_success;
  }
}
return qe_failure;
}

RC Helper::sizeOfTuple(void *data,vector<Attribute> attrs,int &size)
{
//RM *_rm = RM::Instance();
int offset = 0;
for(unsigned int i =0 ; i<attrs.size();i++)
{
if(attrs[i].type == TypeInt || attrs[i].type == TypeReal)
{
offset += size_int;
}
else
{
int stringLength = 0;
writeToBuffer(&stringLength,data,0,offset,size_int);
offset += size_int;
offset += stringLength;
}
}
size = offset;
return qe_success;

}

RC Helper::populateAttribute(Iterator *_leftIn,Iterator *_rightIn,vector<Attribute> &_attrs)
{
	vector<Attribute> leftAttrs;
	_leftIn->getAttributes(leftAttrs);
	vector<Attribute> rightAttrs;
	_rightIn->getAttributes(rightAttrs);

	for(unsigned index = 0 ; index < leftAttrs.size() ; index++)
	{
		_attrs.push_back(leftAttrs[index]);
	}

	for(unsigned index = 0 ; index < rightAttrs.size() ; index++)
	{
		_attrs.push_back(rightAttrs[index]);
	}
	return qe_success;
}

HashJoin::HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
        )
{
	_leftIn = leftIn;
	_rightIn = rightIn;
	_condition = condition;
	_numPages = numPages;

	partitions1 = numPages-1; // Grace join step 1
	partitions2 = numPages-2; // Grace join step 2

//	_rm = RM::Instance();
	helper.populateAttribute(_leftIn,_rightIn,_attrs);
	process();

}

RC HashJoin::getHashFileName(string &attrname)
{
	string extension = ".hash";
	attrname = attrname+extension;
}

RC HashJoin::process()
{
	vector<HashPage> LPages,RPages;
	buildHashTable(_leftIn,LPages,_condition.lhsAttr);
	buildHashTable(_rightIn,RPages,_condition.rhsAttr);

	vector<Attribute> lAttrs,rAttrs;
	_leftIn->getAttributes(lAttrs);
	_rightIn->getAttributes(rAttrs);
	//cout<<*(int *)_lvale.data<<endl;

	vector<Value> values;
	for (int index = 0; index < partitions1; ++index) {
		HashPage lpage;
		readBucket(_condition.lhsAttr,lpage,LPages[index].pageNum);
		HashPage rpage;
		readBucket(_condition.rhsAttr,rpage,RPages[index].pageNum);
		vector<Value> lvalues = lpage.values;
		vector<Value> rvalues = rpage.values;
		int rsize = rvalues.size(),lsize = lvalues.size();
		for (int rIndex = 0; rIndex < rsize; ++rIndex) {
			for (int lIndex = 0; lIndex < lsize; ++lIndex) {
				//cout<<"1"<<endl;
				RC rc = helper.CompareValue(lvalues[lIndex].data,lAttrs,rvalues[rIndex].data,rAttrs,_condition);
//				cout<<rc<<endl;
				if(rc==qe_success)
				{
//					cout<<"Entered"<<endl;
					int lsize = 0 , rsize=0;
					helper.sizeOfTuple(lvalues[lIndex].data,lAttrs,lsize);
					helper.sizeOfTuple(rvalues[rIndex].data,rAttrs,rsize);
					HashValue resultVal;
					resultVal.data = malloc(lsize+rsize);
					helper.writeToBuffer(resultVal.data,lvalues[lIndex].data,0,0,lsize);
					helper.writeToBuffer(resultVal.data,rvalues[rIndex].data,lsize,0,rsize);
					resultVal.size = lsize+rsize;
					_values.push_back(resultVal);
				}
				else
				{

				}
			}
		}

	}
	return qe_success;
}

HashJoin::~HashJoin()
{

}



RC HashJoin::buildHashTable(Iterator *In,vector<HashPage> &pages,string attrname)
{
	PF_Manager *pf = PF_Manager::Instance();
	string hashfilename(attrname);
	getHashFileName(hashfilename);
	if(pf->fileExists(hashfilename.c_str()))
	{
		pf->DestroyFile(hashfilename.c_str());
	}
	pf->CreateFile(hashfilename.c_str());
	// HashTable on Left Index
	char data[bufsize];
	vector<Attribute> attrs;
	In->getAttributes(attrs);
	AttrType type;
	for (int var = 1; var <= partitions1; ++var) {
		HashPage page;
		page.size_left = PF_PAGE_SIZE-4;
		pages.push_back(page);
	}
	int i=0;
	while(In->getNextTuple(&data)!=QE_EOF)
	{
		Value value;
		helper.getValueInTuple(&data,attrs,attrname,type,value);
		int hashIndex = HashFunction1(value.data,type);
		int size=0;
		helper.sizeOfTuple(&data,attrs,size);
		value.size = size;
		if(size>pages[hashIndex].size_left)
		{
			int pageId=0;
			writepage(attrname,pages[hashIndex],pageId);
			pages[hashIndex].pageNum.push_back(pageId);
			pages[hashIndex].values.clear();
			pages[hashIndex].size_left = PF_PAGE_SIZE-4;
		}
		value.data= malloc(size);
		helper.writeToBuffer(value.data,&data,0,0,size);
		pages[hashIndex].values.push_back(value);
		pages[hashIndex].size_left -= (size+size_int);
	}
	for (unsigned int index = 0;  index < pages.size(); ++ index) {
		int pageId=0;
		if(pages[index].values.size()>0)
		{
			writepage(attrname,pages[index],pageId);
			pages[index].pageNum.push_back(pageId);
			pages[index].values.clear();
		}
	}
	return qe_success;
}

RC HashJoin::writepage(string filename,HashPage page,int &pageId)
{
	getHashFileName(filename);
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle fileHandle;
	pf->OpenFile(filename.c_str(),fileHandle);
	pageId = fileHandle.GetNumberOfPages();
	char data[PF_PAGE_SIZE];
	fileHandle.AppendPage(&data);
	pf->CloseFile(fileHandle);
	int size = page.values.size();
	helper.writeToBuffer(&data,&size,0,0,size_int);
	int offset = 4;
	for (int index = 0;  index < size; ++index) {
		int tSize = page.values[index].size;
		helper.writeToBuffer(&data,&tSize,offset,0,size_int);
		offset+=size_int;
		helper.writeToBuffer(&data,page.values[index].data,offset,0,tSize);
		offset+=tSize;
	}
	PF_FileHandle f;
	pf->OpenFile(filename.c_str(),f);
	f.WritePage(pageId,&data);
	pf->CloseFile(f);
//	free(data);
	return qe_success;
}

RC HashJoin::readBucket(string filename,HashPage &page,vector<int> pageNums)
{
	int size = pageNums.size();
	for(int index = 0 ; index< size; index++)
	{
		int pageId = pageNums[index];
		readpage(filename,page,pageId);
	}
	return qe_success;
}

RC HashJoin::readpage(string filename,HashPage &page,int pageId)
{
	PF_Manager *pf = PF_Manager::Instance();
	getHashFileName(filename);
	PF_FileHandle fileHandle;
	pf->OpenFile(filename.c_str(),fileHandle);
	char data[PF_PAGE_SIZE];
	fileHandle.ReadPage(pageId,&data);
	int total;
	int size;
	total = 0;
	helper.writeToBuffer(&total,&data,0,0,size_int);
	total += 0;
	int _c = total+0;
	int offset = 4;
//	value.data = malloc(bufsize);
	for (int index = 0;  index < _c; ++index) {
		size = 0;
		helper.writeToBuffer(&size,&data,0,offset,size_int);
		offset+=size_int;
		Value value;
		value.data = malloc(size);
		value.size = size;
		//cout<<"TSize"<<size<<endl;
		helper.writeToBuffer(value.data,&data,0,offset,size);
		offset+=size;
		page.values.push_back(value);
	}
//	void *ptr = page.values[0].data;
	//cout<<"s"<<*(int *)ptr<<endl;
	pf->CloseFile(fileHandle);
//	free(data);
}

int HashJoin::HashFunction1(void *data,AttrType type)
{
	if(type==TypeInt)
	{
		int value = 0;
		helper.writeToBuffer(&value,data,0,0,size_int);
		int hashIndex = value%(partitions1);
		return hashIndex;
	}
	else if(type==TypeReal)
	{
		float value = 0;
		helper.writeToBuffer(&value,data,0,0,size_float);
		int hashIndex = ((int)value)%(partitions1);
		return hashIndex;
	}
	else if(type==TypeVarChar)
	{

		int size=0;
		helper.writeToBuffer(&size,data,0,0,size_int);
		char val[bufsize];
		helper.writeToBuffer(&val,data,0,size_int,size);
		val[size]='\0';
		string svalue(val);
		int value=0;
		for(int i=0;i<svalue.length();i++)
		{
		value+=svalue[i];
		}
		int hashIndex = value%(partitions1);
		return hashIndex;
	}
	return 0;
}

int HashJoin::HashFunction2(void *data,AttrType type)
{
	if(type==TypeInt)
	{
		int value = 0;
		helper.writeToBuffer(&value,data,0,0,size_int);
		int parity = value%parityHashKey;
		int hashIndex = (value+parity)%(partitions2);
		return hashIndex;
	}
	else if(type==TypeReal)
	{
		float value = 0;
		helper.writeToBuffer(&value,data,0,0,size_float);
		int parity = ((int)value)%parityHashKey;
		int hashIndex = ((int)value+parity)%(partitions2);
		return hashIndex;
	}
	else if(type==TypeVarChar)
	{
		 string svalue;
		int size=0;
		helper.writeToBuffer(&size,data,0,0,size_int);
		helper.writeToBuffer(&svalue,data,0,size_int,size);
		int value=0;
		for(int i=0;i<svalue.length();i++)
		{
		value+=svalue[i];
		}
		int hashIndex = value%(partitions1);
		return hashIndex;

	}
	return 0;
}

RC HashJoin::getNextTuple(void *data) {

	if(_values.size()>0)
	{
		helper.writeToBuffer(data,_values.back().data,0,0,_values.back().size);
		_values.pop_back();
		return qe_success;
	}
	return QE_EOF;
}
// For attribute in vector<Attribute>, name it as rel.attr
void HashJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs = _attrs;
}


Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
                  )
{
	_isGroupBy = false;
	_input = input;
	_aggAttr = aggAttr;
	_op = op;

//	_rm = RM::Instance();

	process();
}

        // Extra Credit
Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
		  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
		  Attribute gAttr,                              // The attribute over which we are grouping the tuples
		  AggregateOp op                                // Aggregate operation
)
{
	_isGroupBy = true;
	_input = input;
	_aggAttr = aggAttr;
	_op = op;
	_gAttr = gAttr;

//	_rm = RM::Instance();

	process();
}

Aggregate::~Aggregate()
{

}

RC Aggregate::process()
{
	vector<float> vals;
	if(_isGroupBy)
	{
		//groupby clause is present
		if(_gAttr.type == TypeReal || _gAttr.type == TypeInt)
		{
			map<float,vector<float> > valuemap;
			map<float,vector<float> >::iterator mapiterator;
			char data[bufsize];
			Value value;
			vector<Attribute> attrs;
			value.data = malloc(size_int);
			_input->getAttributes(attrs);
			int _Ivalue=0;
			float _gFValue,_value=0.0;
			while(_input->getNextTuple(&data)!=QE_EOF)
			{
				_Ivalue=0,_value=0.0;
				_helper.getValueInTuple(&data,attrs,_gAttr.name,_gAttr.type,value);
				if(_gAttr.type==TypeInt)
				{
					int temp=0;
					_helper.writeToBuffer(&temp,value.data,0,0,size_float);
					_gFValue = temp+0.0;
				}
				else
				{
					_helper.writeToBuffer(&_gFValue,value.data,0,0,size_float);
				}


				_helper.getValueInTuple(&data,attrs,_aggAttr.name,_aggAttr.type,value);
				if(_aggAttr.type==TypeInt)
				{
					_helper.writeToBuffer(&_Ivalue,value.data,0,0,size_int);
					_value=_Ivalue+0.0;
				}
				else
				{
					_helper.writeToBuffer(&_value,value.data,0,0,size_int);
				}
				vector<float> keyvalues;
				if(valuemap.count(_gFValue)>0)
				{
					keyvalues=valuemap.find(_gFValue)->second;
					valuemap.erase(_gFValue);
				}
				keyvalues.push_back(_value);
				valuemap.insert(pair<float,vector<float> >(_gFValue,keyvalues));
			}
			float result = 0;
			for ( mapiterator = valuemap.begin(); mapiterator != valuemap.end(); mapiterator++ )
			{
				vector<float> vals;
				vals = (*mapiterator).second;
				conditionOperation(result,vals);
				_result.push_back(result);
				_result.push_back((*mapiterator).first);
			}
		}
		else
		{
			//varchar gattr
		}
	}
	else
	{
		if(_aggAttr.type == TypeInt)
		{
			vector<float> vals;
			char data[bufsize];
			Value value;
			value.data = malloc(size_int);
			vector<Attribute> attrs;
			_input->getAttributes(attrs);
			int _value=0;
			while(_input->getNextTuple(&data)!=QE_EOF)
			{
				_helper.getValueInTuple(&data,attrs,_aggAttr.name,_aggAttr.type,value);
				_helper.writeToBuffer(&_value,value.data,0,0,size_int);
				vals.push_back(_value+0.0);
			}
//			free(data);
			if(vals.size()==0) return QE_EOF;
			float result=0;
			conditionOperation(result,vals);
			_result.push_back(result);

		}else if(_aggAttr.type == TypeReal)
		{
			vector<float> vals;
			char data[bufsize];
			Value value;
			value.data = malloc(size_float);
			vector<Attribute> attrs;
			_input->getAttributes(attrs);
			while(_input->getNextTuple(&data))
			{
				_helper.getValueInTuple(&data,attrs,_aggAttr.name,_aggAttr.type,value);
				float val=0;
				_helper.writeToBuffer(&val,value.data,0,0,size_int);
				vals.push_back(val);
			}
//			free(data);

			Value val;
			val.data = malloc(size_float);
			float result=0;
			conditionOperation(result,vals);
			_result.push_back(result);
		}
	}
	return qe_success;
}

RC Aggregate::conditionOperation(float &result,vector<float> vals)
{
	if(_op == MAX)
	{
		float maxVal = vals[0];
		for(unsigned index = 0 ; index < vals.size() ; index++)
		{
			if(maxVal < vals[index]) maxVal =vals[index];
		}
		result = maxVal;
	}
	else if(_op == MIN)
	{
		float minVal = vals[0];
		for(unsigned index = 0 ; index < vals.size() ; index++)
		{
			if(minVal > vals[index]) minVal =vals[index];
		}
		result = minVal;
	}
	else if(_op == SUM)
	{
		float sum = 0;
		for(unsigned index = 0 ; index < vals.size() ; index++)
		{
			sum +=vals[index];
		}
		result = sum;
	}
	else if(_op == AVG)
	{
		float sum = 0;
		for(unsigned index = 0 ; index < vals.size() ; index++)
		{
			sum +=vals[index];
		}
		float avgVal = 0.0;
		avgVal = (sum+avgVal)/vals.size();
		result = avgVal;
	}
	else if(_op == COUNT)
	{
		int count = vals.size();
		result = count;
	}
	return qe_success;
}

RC Aggregate::getNextTuple(void *data) {
	float val ;
	if(_isGroupBy)
	{
		if(!_result.empty())
		{
			val = _result.back();
			_result.pop_back();
			_helper.writeToBuffer(data,&val,0,0,size_float);
			val = _result.back();
			_result.pop_back();
			_helper.writeToBuffer(data,&val,size_float,0,size_float);
			return qe_success;
		}
		else
		{
			return QE_EOF;
		}
	}
	else
	{
		if(!_result.empty())
		{
			val = _result.back();
			_result.pop_back();
			_helper.writeToBuffer(data,&val,0,0,size_float);
			return qe_success;
		}
		else
		{
			return QE_EOF;
		}
	}
}
// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{

}
