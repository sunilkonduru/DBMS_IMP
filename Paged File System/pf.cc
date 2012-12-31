#include "pf.h"
#include <fstream>
#include <cstdio>
#include <sys/stat.h>
#include <time.h>
#include <stdlib.h>
#include <iostream>

const int success = 0;
const int failure = -1;
const char *PF_Manager::_PF_Metadata_File="metadata";
PF_Manager* PF_Manager::_pf_manager = 0;

PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
	if(!fileExists(_PF_Metadata_File))
	{
		ofstream str;
		str.open(_PF_Metadata_File);
		str.close();
	}
}


PF_Manager::~PF_Manager()
{

}

    
RC PF_Manager::CreateFile(const char *fileName)
{
	if(fileExists(fileName))
	{
		return failure;
	}
	else
	{
		ofstream myfile;
		myfile.open(fileName);
		myfile.close();
//		WriteInMetadata(fileName);
		return success;
	}
    return failure;
}


RC PF_Manager::DestroyFile(const char *fileName)
{
//	RemoveFileFromMetadata(fileName);
	if(remove(fileName)!=0)
	{
		//permission denied or file does not exist
		perror("File not deleted");
		return failure;
	}
    return success;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	// File does not exist or is not created by CreateFile
	if(!fileExists(fileName))
	{
		return failure;
	}
	else
	{
//		if( CheckFileInMetadata(fileName))
//		{
			return fileHandle.OpenFiles(fileName);
//		}
//		else
//		{
//			return failure;
//		}
	}
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if(fileHandle.CloseFiles()==success)
	{
		//update timestamp in metadata file
//		RemoveFileFromMetadata(fileHandle._fileName.c_str()); // remove old entry with past timestamp
//		WriteInMetadata(fileHandle._fileName.c_str()); // add new entry of file with new timestamp
		return success;
	}
	else
	{
		return failure;
	}
}

bool PF_Manager::fileExists(const char* fileName)
{
	ifstream ifile(fileName);
	if(ifile) return true;
	else return false;
}


bool PF_Manager::CheckFileInMetadata(const char *fileName)
{
	ifstream ifile(_PF_Metadata_File);
	string search(fileName) ,line;
	struct stat attrib;
	stat(fileName,&attrib);
	void *data = malloc(20);
	strftime((char *)data,20,"%y.%m.%d.%H.%M.%S",localtime(&(attrib.st_mtime)));
	search = search +" "+(char *)data;
	if(ifile.is_open())
		{
			while(ifile.good())
			{
				getline(ifile,line);
				if((search==line)!=0)
				{
					ifile.close();
					return true;
				}
				else
				{
					// do nothing
				}
			}
		}
		ifile.close();
	return false;
}

PF_FileHandle::PF_FileHandle()
{
}
 

PF_FileHandle::~PF_FileHandle()
{
	if(_filestr.is_open())
	{
		_filestr.close();
	}
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if(_filestr.is_open() && pageNum<GetNumberOfPages())
	{
		_filestr.seekg(pageNum*PF_PAGE_SIZE,ios::beg);
		_filestr.read((char *)data,PF_PAGE_SIZE);
		return success;
	}
	else
	{
		//Trying to read a page from an empty file stream or a page not present in file
		return failure;
	}
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if(_filestr.is_open() && pageNum<GetNumberOfPages())
	{
		_filestr.seekp(pageNum*PF_PAGE_SIZE,ios::beg);
		_filestr.write((char *)data,PF_PAGE_SIZE);
		return success;
	}
	else
	{
		//Trying to write a page from an empty file stream or a page not present in file
		return failure;
	}
}


RC PF_FileHandle::AppendPage(const void *data)
{
	if(_filestr.is_open())
	{
		_filestr.seekp(GetNumberOfPages()*PF_PAGE_SIZE,ios::beg);
		_filestr.write((char *)data,PF_PAGE_SIZE);
		return success;
	}
	else
	{
		return failure;
	}
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	if(_filestr.is_open())
	{
		_filestr.seekg(0,ios::end);
		unsigned int size = _filestr.tellg()/PF_PAGE_SIZE;
		_filestr.seekg(0,ios::beg);
		return size;
	}
	else
	{
		return -1;
	}
}

RC PF_FileHandle::OpenFiles(const char *fileName)
{
	if(!_filestr.is_open())
	{
		_filestr.open(fileName,ios::in|ios::out|ios::binary);
		_fileName = fileName;
		return success;
	}
	else
	{
		//file handle is already open on another filestream
		return failure;
	}
}

RC PF_FileHandle::CloseFiles()
{
	if(_filestr.is_open())
	{
		_filestr.close();
		return success;
	}
	else
	{
		//Trying to close a filestream which is null
		return failure;
	}
}


