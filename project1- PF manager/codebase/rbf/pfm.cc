#include "pfm.h"


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	ifstream file(fileName); //to check if the file already exists!
	if (file.good())
    {
		//file.close();
		cout << "Tried creating duplicate file!" << endl << endl;
		return -1;
    }
	else
	{
		ofstream outfile(fileName);
		return 0;
	}
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	// 1. to check if file exists

	ifstream file(fileName);
	if (!file.good())
	{
		//file.close();
		return -1;
	}
	//2. to open a file
	FILE * myFile = fopen(fileName.c_str(),"rb+wb");
	if(myFile==NULL)
	{
		return -1;
	}

	//3. FileHandle checking

	if(fileHandle.getFilePtr() != NULL)  // Checking if fileHandle is free
	{
		cout << "FileHandle busy!!";
		return -1;
	}
	if(fileHandle.getFileName() != "")  // Checking if fileHandle is connected with other files
	{
		cout << "The fileHandle has already been connected with other files";
		return -1;
	}
	fileHandle.setFileName(fileName);	//setting the name
	fileHandle.setFilePtr(myFile);		// connecting fileHandle to  "myFile"
	return 0;
	//return fileHandle.open(fileName);
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	//return fileHandle.close();
	FILE* fp = fileHandle.getFilePtr();	//getting the file attached to the fileHandle
	if (fclose(fp) != 0)		// closing the file connected to the fileHandle (myFile)
	{
		return -1;
	}

	fileHandle.setFilePtr(NULL);		// setting the pointer back to NULL
	return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    file = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	fseek(file, pageNum*PAGE_SIZE, 0);
	if(fread(data, PAGE_SIZE, 1, file) != 1)
	{
	   return -1;
	}
	 readPageCounter++;
	 return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    //return -1;
	if(pageNum > getNumberOfPages())
	{
		cout <<"Page does not exist!";
		return -1;
	}
	if (fseek(file, pageNum*PAGE_SIZE, 0) != 0)
	{
		cout << "Seek-page error";
		return -1;
	}
	if(fwrite(data,PAGE_SIZE,1,file) != 1)
	{
		cout<< "writePage Failure";
		return -1;
	}
	writePageCounter++;
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
	 fseek(file, 0, SEEK_END);
	 if(fwrite(data,PAGE_SIZE,1,file) != 1)
	 {
		 return -1;
	 }
	 appendPageCounter++;
	 return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    //return -1;
	fseek(file,0,SEEK_END);
	long size = ftell(file);
	return size/PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;
}
