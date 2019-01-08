#include "rbfm.h"
#include <math.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstring>

#define Bit_size2 sizeof(unsigned short)
#define Bit_size_int  sizeof(int)
#define Bit_size_float sizeof(float)
#define Bool_size sizeof(bool)
RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager *RecordBasedFileManager::instance() {
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {
}


RC RecordBasedFileManager::createFile(const string &fileName) {
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
                                    FileHandle &fileHandle) {
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}
RC CalculateRecordLength(const vector<Attribute>recordDescriptor,const void *data,
						unsigned char nullsIndicator[], unsigned short &dataLen,
						 unsigned short locations[],size_t nullBitsLen )
{
	 bool nullBit;
	    for (int j = 0; j < recordDescriptor.size(); j++) {

	        nullBit = (bool) ((nullsIndicator[j / CHAR_BIT]) & (1 << (7 - (j % CHAR_BIT))));
	        if (!nullBit) {

	        	 locations[j] = Bool_size + (unsigned short) (recordDescriptor.size() * Bit_size2) + dataLen;
	        	        switch (recordDescriptor[j].type) {
	        	            case TypeInt: {
	        	                dataLen +=Bit_size_int;
	        	                break;
	        	            }
	        	            case TypeReal: {
	        	                dataLen += Bit_size_float;
	        	                break;
	        	            }
	        	            case TypeVarChar: {
	        	                //Get length of varchar
	        	                int length = 0;
	        	                memcpy(&length, (char *) data + nullBitsLen + dataLen,Bit_size_int);
	        	                dataLen +=Bit_size_int;
	        	                dataLen += length;
	        	                break;
	        	            }

	        }
	        }
	        	        else{
	        	        	 locations[j] = 0;

	        	        }


	    }


	}
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    //Calculate length of data
    size_t nullBitsLen = (size_t)(int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullsIndicator[nullBitsLen];
    memcpy(nullsIndicator, data, nullBitsLen);

    unsigned short dataLen = 0;
    unsigned short locations[recordDescriptor.size()];
    unsigned short locationDirLen = (unsigned short) (recordDescriptor.size() * Bit_size2);

    CalculateRecordLength(recordDescriptor,data,nullsIndicator,dataLen,locations,nullBitsLen);
    // create record format
    unsigned short recordLen = Bool_size + locationDirLen + dataLen;
    bool fwd = false;
    void *newRecord = malloc(recordLen);
    memcpy(newRecord, &fwd, Bool_size);  //copy fwd flag
    memcpy((char *) newRecord + Bool_size, locations, locationDirLen); //copy location dir
    memcpy((char *) newRecord + Bool_size + locationDirLen, (char *) data + nullBitsLen,
           (size_t) (dataLen)); //copy values


    //Find Page
    unsigned pageNum;
    pageNum = 0;
    void *page = malloc(PAGE_SIZE);
    bool flag=false;
    int total_page=fileHandle.getNumberOfPages();
    if (total_page > 0) {
        do {
            fileHandle.readPage(pageNum, page);
            unsigned short freeSpaceLocation, recordCount;
            getFreeSpacePointer(page, freeSpaceLocation);
            getRecordCount(page, recordCount);

            unsigned short directoryLocation = (PAGE_SIZE - ((recordCount + 2) * 2 * Bit_size2));
            if (directoryLocation - freeSpaceLocation > recordLen) {
            	int k;
            	    	            	for(k=1;k<=recordCount;k++){
            	    	            	unsigned short length,location;
            	    	            	unsigned short length2,location2;
            	    	                memcpy(&location, (char *) page + REC_COUNT_OFFSET - (k * 2 * sizeof(unsigned short)),
            	    	            	            		           sizeof(unsigned short));
            	    	                memcpy(&length,(char *) page + REC_COUNT_OFFSET - (k * 2 * sizeof(unsigned short)) + sizeof(unsigned short),
            	    	            	            		             sizeof(unsigned short));
            	    	                memcpy(&location2, (char *) page + REC_COUNT_OFFSET - ((k+1) * 2 * sizeof(unsigned short)),
            	    	                        	             			             		           sizeof(unsigned short));
            	    	            	if(length==0)
            	    	            	{
            	    	            		 flag=false;

            	    	            	     //length2=location2-location;



            	    	            		 memcpy(&freeSpaceLocation, (char *) page + FREE_SLOT_OFFSET, sizeof(unsigned short));
            	    	            	     memmove((char *) page + location + recordLen, (char *) page + location,
            	    	            	            			                 freeSpaceLocation - location);
            	    	            	     for (int i = k + 1; i <= recordCount; i++)
            	    	            	     {
            	    	            	            	unsigned short newLocation, oldLocation;
            	    	            	            	memcpy(&oldLocation, (char *) page + REC_COUNT_OFFSET - (i * 2 * sizeof(unsigned short)),
            	    	            	            			            			                    sizeof(unsigned short));
            	    	            	            	newLocation = oldLocation + (recordLen - length2);
            	    	            	            	memcpy((char *) page + REC_COUNT_OFFSET - (i * 2 * sizeof(unsigned short)), &newLocation,
            	    	            	            			            			                    sizeof(unsigned short));
            	    	            	     }
            	    	            	     unsigned short freeSpaceLocation;
            	    	            	     //memcpy(&freeSpaceLocation, (char *) page + FREE_SLOT_OFFSET, sizeof(unsigned short));

            	    	            	     memcpy((char *) page + location, newRecord, (size_t) recordLen);

            	    	            	     unsigned short recordCount;
            	    	            	     // memcpy(&recordCount, (char *) page + REC_COUNT_OFFSET, sizeof(unsigned short));
            	    	            	     // recordCount++;
            	    	            	      rid.pageNum = pageNum;
            	    	            	      rid.slotNum = k;
            	    	            	      setRecordSlot(page, rid, recordLen, freeSpaceLocation);

            	    	            	                             //Update free space and record count
            	    	            	      freeSpaceLocation += recordLen;
            	    	            	      memcpy((char *) page + FREE_SLOT_OFFSET, &freeSpaceLocation, sizeof(unsigned short));
            	    	            	      memcpy((char *) page + REC_COUNT_OFFSET, &recordCount, sizeof(unsigned short));

            	    	            	        //Write to file
            	    	            	        RC r = fileHandle.writePage(pageNum, page);
            	    	            	//        free(page);
            	    	            	//        free(newRecord);
            	    	            	        return r;
            	    	            	}
            	    	           }

            	    	            	flag=true;
            	    	            	break;
            } else {
                pageNum++;
                if (pageNum >= total_page) {
                    fileHandle.appendPage(page);
                    unsigned short free = 0;
                      unsigned short recordCount = 0;
                      memcpy((char *) page + FREE_SLOT_OFFSET, &free, Bit_size2);
                      memcpy((char *) page + REC_COUNT_OFFSET, &recordCount, Bit_size2);
                      flag=true;
                    break;
                }
            }
        } while (pageNum <total_page);
    } else {
        fileHandle.appendPage(page);
        unsigned short free = 0;
          unsigned short recordCount = 0;
          memcpy((char *) page + FREE_SLOT_OFFSET, &free, Bit_size2);
          memcpy((char *) page + REC_COUNT_OFFSET, &recordCount, Bit_size2);
          flag=true;
    }

    //Find free space in page
if(flag==true)
{
    unsigned short freeSpaceLocation;
    memcpy(&freeSpaceLocation, (char *) page + FREE_SLOT_OFFSET, Bit_size2);

    //Write data in free space
    memcpy((char *) page + freeSpaceLocation, newRecord, (size_t) recordLen);

    //Update Directory
    unsigned short recordCount;
    memcpy(&recordCount, (char *) page + REC_COUNT_OFFSET, Bit_size2);
    recordCount++;
    rid.pageNum = pageNum;
    rid.slotNum = (unsigned) recordCount;
    setRecordSlot(page, rid, recordLen, freeSpaceLocation);

    //Update free space and record count
    freeSpaceLocation += recordLen;
    memcpy((char *) page + FREE_SLOT_OFFSET, &freeSpaceLocation, Bit_size2);
    memcpy((char *) page + REC_COUNT_OFFSET, &recordCount, Bit_size2);

    //Write to file
    RC r = fileHandle.writePage(pageNum, page);
    free(page);
    free(newRecord);
    return r;
}
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    //Read Page
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    //Get record location and length
    unsigned short length, location;
    getRecordSlot(page, rid, length, location);

    if (length == 0) { //deleted record
        return -1;
    }

    //Forwarded record logic
    bool fwd;
    RID newRID;
    isRecordForwarded(page, location, fwd, newRID);
    if (fwd) {
        return readRecord(fileHandle, recordDescriptor, newRID, data);
    }

    unsigned short locations[recordDescriptor.size()];
    unsigned long locationDirLen = (recordDescriptor.size() * Bit_size2);
    size_t nullsLen = (size_t)(int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullsIndicator[nullsLen];
    memset(nullsIndicator, 0, nullsLen);

    void *record = malloc(length);
    memcpy(record, (char *) page + location, length);

    //get locations array
    memcpy(locations, (char *) record + Bool_size, locationDirLen);

    //prepare null bits from locations array
    for (int j = 0; j < recordDescriptor.size(); j++) {
        if (locations[j] == 0) {
            nullsIndicator[j / CHAR_BIT] += (1 << (7 - (j % CHAR_BIT)));
        }
    }

    //return data
    memcpy(data, nullsIndicator, nullsLen);
    memcpy((char *) data + nullsLen, (char *) record + Bool_size + locationDirLen, length - locationDirLen);


//    cout << "Page : " << rid.pageNum << "Loc: " << location << " Len: " << length << endl;
    free(page);
    free(record);
    return 0;
}

RC RecordBasedFileManager::printRecord(
        const vector<Attribute> &recordDescriptor, const void *data) {

    int offset = 0;

    int nullFieldsIndicatorActualSize = (int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memcpy(nullsIndicator, data, (size_t) nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    int nullBitCounter = 0;
    bool nullBit;
    for (Attribute i : recordDescriptor) {
        cout << i.name << " : ";
        nullBit = (bool) ((nullsIndicator[nullBitCounter / CHAR_BIT]) & (1 << (7 - (nullBitCounter % CHAR_BIT))));
        nullBitCounter++;
        if (nullBit) {
            cout << "NULL" << "\t";
            continue;
        }

        switch (i.type) {
            case TypeInt: {
                int intVal = 0;
                memcpy(&intVal, (char *) data + offset,Bit_size_int);
                offset +=Bit_size_int;
                cout << intVal << "\t";
                break;
            }
            case TypeReal: {
                float val = 0;
                memcpy(&val, (char *) data + offset, Bit_size_float);
                offset += Bit_size_float;
                cout << val << "\t";
                break;
            }
            case TypeVarChar: {
                //Get length of varchar
                int length = 0;
                memcpy(&length, (char *) data + offset,Bit_size_int);
                offset +=Bit_size_int;

                //Get actual value of varchar
                char val[length + 1];
                memcpy(val, (char *) data + offset, (size_t) length);
                val[length] = '\0';
                offset += length;
                cout << val << "\t";
                break;
            }
        }
    }
    cout << endl;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);


    unsigned short length, location, freeSpacePointer;
    getRecordSlot(page, rid, length, location);
    getFreeSpacePointer(page, freeSpacePointer);

    //Forwarded record logic
    bool fwd;
    RID newRID;
    isRecordForwarded(page, location, fwd, newRID);
    if (fwd) {
        deleteRecord(fileHandle, recordDescriptor, newRID);
    }

    unsigned short recordCount;
    getRecordCount(page, recordCount);
    makeSpaceForRecord(length, 0, page, location, freeSpacePointer, recordCount, rid);
    length = 0;
    setRecordSlot(page, rid, length, location);
    setFreeSpacePointer(page, freeSpacePointer);

    RC code = fileHandle.writePage(rid.pageNum, page);
    free(page);
    return code;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                         const RID &rid, const string &attributeName, void *data) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    unsigned short length, location;
    getRecordSlot(page, rid, length, location);

    //Forwarded record logic
    bool fwd;
    RID newRID;
    isRecordForwarded(page, location, fwd, newRID);
    if (fwd) return readAttribute(fileHandle, recordDescriptor, newRID, attributeName, data);

    unsigned short locations[recordDescriptor.size()];
    unsigned long locationDirLen = (recordDescriptor.size() * Bit_size2);

    memcpy(locations, (char *) page + location + Bool_size, locationDirLen);
    int i = 0;
    do {
        if (strcmp(recordDescriptor[i].name.c_str(), attributeName.c_str()) == 0) {
            if (locations[i] == 0) {
                char nulls = (char) (1 << 7);
                memcpy(data, &nulls, sizeof(char));
            } else {
                char nulls = 0;
                memcpy(data, &nulls, sizeof(char));
                switch (recordDescriptor[i].type) {
                    case TypeInt: {
                        memcpy(data, &nulls, sizeof(char));
                        memcpy((char *) data + sizeof(char), (char *) page + location + locations[i],Bit_size_int);
                        break;
                    }
                    case TypeReal: {
                        memcpy(data, &nulls, sizeof(char));
                        memcpy((char *) data + sizeof(char), (char *) page + location + locations[i], Bit_size_float);
                        break;
                    }
                    case TypeVarChar: {
                        int varCharLen = 0;
                        memcpy(&varCharLen, (char *) page + location + locations[i],Bit_size_int);
                        memcpy(data, &nulls, sizeof(char));
                        memcpy((char *) data + sizeof(char), &varCharLen,Bit_size_int);
                        memcpy((char *) data + sizeof(char) +Bit_size_int,
                               (char *) page + location + locations[i] +Bit_size_int, (size_t) varCharLen);
                        break;
                    }
                }
            }
            free(page);
            return 0;
        }
        i++;
    } while (i < recordDescriptor.size());
    free(page);
//    cout << "ERROR : " << attributeName << " not found in descriptor! \n";
    return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {

    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    unsigned short length, location;
    getRecordSlot(page, rid, length, location);

    //Forwarded record logic
    bool fwd1;
    RID newRID;
    isRecordForwarded(page, location, fwd1, newRID);
    if (fwd1) return updateRecord(fileHandle,recordDescriptor,data,newRID);

    //Calculate length of new data
    size_t nullBitsLen = (size_t)(int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullsIndicator[nullBitsLen];
    memcpy(nullsIndicator, data, nullBitsLen);

    unsigned short dataLen = 0;
    unsigned short locations[recordDescriptor.size()];
    unsigned short locationDirLen = (unsigned short) (recordDescriptor.size() * Bit_size2);


    CalculateRecordLength(recordDescriptor,data,nullsIndicator,dataLen,locations,nullBitsLen);



    //Is there space for updated record?
    unsigned short recordLen = Bool_size + locationDirLen + dataLen;
    unsigned short freeSpaceLocation, recordCount;
    getFreeSpacePointer(page, freeSpaceLocation);
    getRecordCount(page, recordCount);

    unsigned short directoryLocation = (PAGE_SIZE - ((recordCount + 2) * 2 * Bit_size2));
    bool fwd = false;
    if ((recordLen > length) && (directoryLocation - freeSpaceLocation + length < recordLen)) {
        fwd = true;
        int tombStoneSize = Bool_size + sizeof(RID);
        makeSpaceForRecord(length, tombStoneSize, page, location, freeSpaceLocation, recordCount, rid);
    } else {
        makeSpaceForRecord(length, recordLen, page, location, freeSpaceLocation, recordCount, rid);
    }
    setFreeSpacePointer(page, freeSpaceLocation);
    //prepare record

    if (fwd) {
        RID newRID;
        if (insertRecord(fileHandle, recordDescriptor, data, newRID) != 0)
            return -1;
        memcpy((char *) page + location, &fwd, Bool_size);
        memcpy((char *) page + location + Bool_size, &newRID, sizeof(rid));
        length = Bool_size + sizeof(RID);
    } else {
        void *record = malloc(recordLen);
        memcpy(record, &fwd, Bool_size);  //copy fwd flag
        memcpy((char *) record + Bool_size, locations, locationDirLen); //copy location dir
        memcpy((char *) record + Bool_size + locationDirLen, (char *) data + nullBitsLen,
               (size_t) (dataLen)); //copy values
        memcpy((char *) page + location, record, recordLen);
        length = recordLen;
        free(record);
    }
    setRecordSlot(page, rid, length, location);
    RC rc = fileHandle.writePage(rid.pageNum, page);
    free(page);
    return rc;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,
                                const void *value,
                                const vector<string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator) {
    RBFM_ScanIterator *scanIterator = new RBFM_ScanIterator();


    scanIterator->initialize(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, *this);
    rbfm_ScanIterator = *scanIterator;
    return 0;
}




void RecordBasedFileManager::getRecordSlot(void *page, RID rid, unsigned short &length, unsigned short &location) {
    memcpy(&location, (char *) page + REC_COUNT_OFFSET - (rid.slotNum * 2 * Bit_size2),
           Bit_size2);
    memcpy(&length,
           (char *) page + REC_COUNT_OFFSET - (rid.slotNum * 2 * Bit_size2) + Bit_size2,
           Bit_size2);
}

void
RecordBasedFileManager::setRecordSlot(void *page, RID rid, const unsigned short length, const unsigned short location) {
    memcpy((char *) page + REC_COUNT_OFFSET - (rid.slotNum * 2 * Bit_size2), &location,
           Bit_size2);
    memcpy((char *) page + REC_COUNT_OFFSET - (rid.slotNum * 2 * Bit_size2) + Bit_size2,
           &length, Bit_size2);
}


void RecordBasedFileManager::getFreeSpacePointer(void *page, unsigned short &freeSpaceLocation) {
    memcpy(&freeSpaceLocation, (char *) page + FREE_SLOT_OFFSET, Bit_size2);
}

void RecordBasedFileManager::getRecordCount(void *page, unsigned short &recordCount) {
    memcpy(&recordCount, (char *) page + REC_COUNT_OFFSET, Bit_size2);
}

void RecordBasedFileManager::setFreeSpacePointer(void *page, unsigned short &freeSpaceLocation) {
    memcpy((char *) page + FREE_SLOT_OFFSET, &freeSpaceLocation, Bit_size2);
}

void RecordBasedFileManager::setRecordCount(void *page, unsigned short &recordCount) {
    memcpy((char *) page + REC_COUNT_OFFSET, &recordCount, Bit_size2);
}


void
RecordBasedFileManager::makeSpaceForRecord(int oldLength, int recordLen, void *page, int location,
                                           unsigned short &freeSpaceLocation, int recordCount, const RID rid) {
    if (recordLen > oldLength) {
        //move records to make space and update directory
        memmove((char *) page + location + recordLen, (char *) page + location + oldLength,
                freeSpaceLocation - location - oldLength);
        for (int i = rid.slotNum + 1; i <= recordCount; i++) {
            unsigned short newLocation, oldLocation;
            memcpy(&oldLocation, (char *) page + REC_COUNT_OFFSET - (i * 2 * Bit_size2),
                   Bit_size2);
            newLocation = oldLocation + (recordLen - oldLength);
            memcpy((char *) page + REC_COUNT_OFFSET - (i * 2 * Bit_size2), &newLocation,
                   Bit_size2);
        }
        freeSpaceLocation += (recordLen - oldLength);
    } else if (oldLength > recordLen) {
        //compact space in page
        memmove((char *) page + location + recordLen, (char *) page + location + oldLength,
                freeSpaceLocation - location - oldLength);
        for (int i = rid.slotNum + 1; i <= recordCount; i++) {
            unsigned short newLocation, oldLocation;
            memcpy(&oldLocation, (char *) page + REC_COUNT_OFFSET - (i * 2 * Bit_size2),
                   Bit_size2);
            newLocation = oldLocation - (oldLength - recordLen);
            memcpy((char *) page + REC_COUNT_OFFSET - (i * 2 * Bit_size2), &newLocation,
                   Bit_size2);
        }
        freeSpaceLocation -= (oldLength - recordLen);
    }

}

void RecordBasedFileManager::isRecordForwarded(void *page, int location, bool &fwd, RID &newRID) {
    memcpy(&fwd, (char *) page + location, Bool_size);
    if (fwd) {
        memcpy(&newRID, (char *) page + location + Bool_size, sizeof(RID));
    }
}


/***********************************************************************************************************************
 * RBFM Scan Iterator
 **********************************************************************************************************************/

void RBFM_ScanIterator::initialize(FileHandle &fileHandle,
                                   const vector<Attribute> &recordDescriptor,
                                   const string &conditionAttribute,
                                   const CompOp compOp,
                                   const void *value,
                                   const vector<string> &attributeNames,
                                   RecordBasedFileManager &rbfm) {
    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->conditionAttribute = conditionAttribute;
    this->compOp = compOp;
    this->value = value;
    this->attributeNames = attributeNames;
    this->rbfm = &rbfm;
    currentRID.pageNum = 0;
    currentRID.slotNum = 0;
    currentPage = malloc(PAGE_SIZE);
    totalPages = fileHandle.getNumberOfPages();
    fileHandle.readPage(0, currentPage);
    rbfm.getRecordCount(currentPage, recordsInCurrentPage);

};

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    do {
        currentRID.slotNum++;
        if (currentRID.slotNum > recordsInCurrentPage) {
            currentRID.pageNum++;
            if (currentRID.pageNum >= totalPages) {
                return RBFM_EOF;
            } else {
                fileHandle.readPage(currentRID.pageNum, currentPage);
                rbfm->getRecordCount(currentPage, recordsInCurrentPage);
                currentRID.slotNum = 1;
            }
        }

        unsigned short length, location;
        rbfm->getRecordSlot(currentPage, currentRID, length, location);
        if (length == 0) { //deleted record
            continue;
        }
        void *record = malloc(length);
        if (rbfm->readRecord(fileHandle, recordDescriptor, currentRID, record) != 0)
            continue;

        void *attribute = malloc(1000);
        if (conditionAttribute.compare("") != 00){

            if(rbfm->readAttribute(fileHandle, recordDescriptor, currentRID, conditionAttribute, attribute) != 0)
                continue;

            char isNull = 0;
            memcpy(&isNull, attribute, sizeof(char));
            if (isNull & (1 << 7)) {
                continue;
            }

            int position = 0;
            while (position < recordDescriptor.size()) {
                if (strcmp(recordDescriptor[position].name.c_str(), conditionAttribute.c_str()) == 0) {
                    break;
                }
                position++;
            }

            bool skip = false;
            if (compOp != NO_OP) {
                CompOp relation;
                switch (recordDescriptor[position].type) {
                    case TypeInt: {
                        int num1, num2;
                        memcpy(&num2,(char *) value,Bit_size_int);
                        memcpy(&num1, (char *) attribute + sizeof(char),Bit_size_int);
                        relation = compareNumbers(num1, num2);
                        break;
                    }
                    case TypeReal: {
                        float num1, num2;
                        memcpy(&num2,(char *) value, Bit_size_float);
                        memcpy(&num1, (char *) attribute + sizeof(char), Bit_size_float);
                        relation = compareNumbers(num1, num2);
                        break;
                    }
                    case TypeVarChar: {
                        int vLen = 0;
                        memcpy(&vLen, (char *) attribute + sizeof(char),Bit_size_int);
                        char str2[length + 1];
                        memcpy(&str2, (char *) attribute + sizeof(char) +Bit_size_int, (size_t) vLen);
                        str2[vLen] = '\0';

                        int vlen2;
                        memcpy(&vlen2, value,Bit_size_int);
                        char s[vlen2 + 1];
                        memcpy(&s,(char *) value +Bit_size_int, vlen2);
                        s[vlen2] = '\0';
                        relation = compareString(str2, s);
                    }
                }
                if (compOp != relation) {
                    if (compOp == LE_OP && (relation == LT_OP || relation == EQ_OP)) {}
                    else if (compOp == GE_OP && (relation == GT_OP || relation == EQ_OP)) {}
                    else if (compOp == NE_OP && (relation == LT_OP || relation == GT_OP)) {}
                    else skip = true;
                }
            }

            if (skip) {
                continue;
            }

        }

        size_t nullBitsLen = (size_t)(int) ceil((double) attributeNames.size() / CHAR_BIT);
        unsigned char nullsIndicator[nullBitsLen];
        memset(nullsIndicator, 0, nullBitsLen);
        int dataLen = (int) nullBitsLen;
        for (int i = 0; i < attributeNames.size(); i++) {
            void *tempAttr = malloc(1000);
            rbfm->readAttribute(fileHandle, recordDescriptor, currentRID, attributeNames[i], tempAttr);
            char isNull;
            memcpy(&isNull, tempAttr, sizeof(char));
            if (isNull & (1 << 7)) {
                nullsIndicator[i / CHAR_BIT] += (1 << (7 - (i % CHAR_BIT)));
                continue;
            } else {
                int position = 0;
                while (position < recordDescriptor.size()) {
                    if (strcmp(recordDescriptor[position].name.c_str(), attributeNames[i].c_str()) == 0) {
                        switch (recordDescriptor[position].type) {
                            case TypeInt: {
                                memcpy((char *) data + dataLen, (char *) tempAttr + sizeof(char),Bit_size_int);
                                dataLen +=Bit_size_int;
                                break;
                            }
                            case TypeReal: {
                                memcpy((char *) data + dataLen, (char *) tempAttr + sizeof(char), Bit_size_float);
                                dataLen += Bit_size_float;
                                break;
                            }
                            case TypeVarChar: {
                                int vlen;
                                memcpy(&vlen, (char *) tempAttr + sizeof(char),Bit_size_int);
                                memcpy((char *) data + dataLen, (char *) tempAttr + sizeof(char),Bit_size_int + vlen);
                                dataLen +=Bit_size_int + vlen;
                                break;
                            }
                        }
                        break;
                    }
                    position++;
                }
            }
        }
        memcpy(data, nullsIndicator, nullBitsLen);
        rid = currentRID;
        return 0;

    } while (currentRID.pageNum < totalPages);
    return RBFM_EOF;
};

RC RBFM_ScanIterator::close() {
    return fileHandle.close();
}

CompOp RBFM_ScanIterator::compareNumbers(float num1, float num2) {
    if (num1 > num2) return GT_OP;
    if (num1 < num2) return LT_OP;
    return EQ_OP;
}

CompOp RBFM_ScanIterator::compareString(const char *str1, const char *str2) {
    if (strcmp(str1, str2) > 0) return GT_OP;
    if (strcmp(str1, str2) < 0) return LT_OP;
    return EQ_OP;
}
