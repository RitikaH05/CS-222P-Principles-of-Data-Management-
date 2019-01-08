
#include "ix.h"

#include "../rbf/rbfm.h"
//Below two added for windows compatibility
#include<stdlib.h>
#include <stdint.h>
#define size_pagenum sizeof(PageNum)
#define Bit_size2 sizeof(unsigned short)
#define Bit_size_int sizeof(int)
#define Bit_size_float sizeof(float)
#define Bool_size sizeof(bool)
#define Node_Type_size sizeof(NodeType)
#define initialize_page malloc(PAGE_SIZE)
IndexManager *IndexManager::_index_manager = 0;

IndexManager *IndexManager::instance() {
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager() {
    pagedFileManager = PagedFileManager::instance();
}

IndexManager::~IndexManager() {
}


void getNumKeys(unsigned short &numKeys, const void *page){
    memcpy(&numKeys, (char *) page + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2)), Bit_size2);
}

void setNumKeys(unsigned short numKeys, const void *page) {
    memcpy((char *) page + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2)), &numKeys, Bit_size2);
}
RC IndexManager::createFile(const string &fileName) {
    return pagedFileManager->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName) {
    return pagedFileManager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
    return ixfileHandle.fileHandle.open(fileName);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
    return ixfileHandle.fileHandle.close();
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {

    PageNum pageNum = 0;
    void *rootPage = initialize_page;
    void *intermediateKey;
    PageNum IntermediatePageNum;
    void *new_root = initialize_page;
    
    unsigned short FSPointer = 0;
    unsigned short freeSpace = 0;
    unsigned short nKeys = 0;
    if (ixfileHandle.getNumberOfPages() == 0) {
      
           NodeType type = TypeLeaf;
           nKeys=0;
           PageNum nextLeaf = 0;

           memcpy((char *) rootPage + (PAGE_SIZE - Node_Type_size), &type, Node_Type_size);
           memcpy((char *) rootPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpace, Bit_size2);
           memcpy((char *) rootPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2)), &nKeys, Bit_size2);
           memcpy((char *) rootPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - sizeof(PageNum)), &nextLeaf, size_pagenum);
        ixfileHandle.appendPage(rootPage);
    }
    ixfileHandle.readPage(pageNum, rootPage);


    RC rc = insertInPage(ixfileHandle, attribute, key, rid, rootPage, pageNum, intermediateKey, IntermediatePageNum);
    if (rc == 1) {

        NodeType type = TypeNonLeaf;
        nKeys=0;
        memcpy((char *) new_root + (PAGE_SIZE - Node_Type_size), &type, Node_Type_size);
        memcpy((char *) new_root + (PAGE_SIZE - Node_Type_size - Bit_size2), &FSPointer, Bit_size2);
        memcpy((char *) new_root + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2)), &nKeys, Bit_size2);
        PageNum leftPage = (PageNum) ixfileHandle.getNumberOfPages();
        memcpy(new_root, &leftPage, size_pagenum);

        int keyLen = getKeyLen(attribute, intermediateKey);
        memcpy((char *) new_root + size_pagenum, intermediateKey, (size_t) keyLen);

        memcpy((char *) new_root + size_pagenum + keyLen, &IntermediatePageNum, size_pagenum);

        unsigned short freeSpacePointer = keyLen + size_pagenum * 2, numKeys = 1;
        //set Free Space Pointer;
        memcpy((char *) new_root + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
        setNumKeys(numKeys, new_root);

        ixfileHandle.writePage(0, new_root);
        ixfileHandle.appendPage(rootPage);
        free(new_root);
        free(intermediateKey);
        rc = 0;
    }
    free(rootPage);
    return rc;

}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    /*
     * 1. Find leaf Node for the key value [get leaf node page number and page pointer]
     * 2. Parse through the leaf node with the search key value (to find the particular key in leaf node).
     * 3. Remove the particular RID and shift the subsequent records.
     * 4. Check for the list of RIDs if any [if not, delete the key and move the subsequent records; else move the subsequent RIDs & records]
     * 5. Set free space pointer and write the new page to disk.
     */
    int key_len = getKeyLen(attribute, key);
    unsigned short freeSpace_ptr;
    int key_offset;
    
    PageNum num_leafPage = 0;
    void *leafPage = malloc(PAGE_SIZE);
    //find leaf for the given Key
    ixfileHandle.readPage(num_leafPage, leafPage);
    NodeType type;
    //get Node Type
    memcpy(&type, (char *) leafPage + (PAGE_SIZE - sizeof(NodeType)), sizeof(NodeType)); //reading type of Node
    int Node_keyOff;
    while (type == TypeNonLeaf) //1. parsing through the non leaf nodes to reach leaf node
    {
        RC rc = fetchKeyOffsetInPage(leafPage, attribute, key, Node_keyOff);
        if (rc == 0) {
            int offsetKeyLen = getKeyLen(attribute, (char *) leafPage + Node_keyOff);
            memcpy(&num_leafPage, (char *) leafPage + Node_keyOff + offsetKeyLen, sizeof(PageNum));
        } else
            memcpy(&num_leafPage, (char *) leafPage + Node_keyOff - sizeof(PageNum), sizeof(PageNum));
        
        ixfileHandle.readPage(num_leafPage, leafPage);
        //get Node Type
        memcpy(&type, (char *) leafPage + (PAGE_SIZE - sizeof(NodeType)), sizeof(NodeType)); //reading type of Node
    }
    
    
    RC rc = fetchKeyOffsetInPage(leafPage, attribute, key, key_offset); //2. parsing through the leaf node to find key offset
    if (rc != 0)
    {
        return -1; //error - key not found
    }
    
    
    
    //get Free Space Pointer
    memcpy(&freeSpace_ptr, (char *) leafPage + (PAGE_SIZE - sizeof(NodeType) - sizeof(unsigned short)), sizeof(unsigned short));
    
    unsigned short RIDlist_size;
    memcpy(&RIDlist_size, (char *) leafPage + key_offset + key_len, sizeof(unsigned short));
    
    int RIDList_offset = key_offset + key_len + sizeof(unsigned short);
    for (int i = 0; i < RIDlist_size; i++)  //3. parsing through list of RIDs to find the particular RID
    {
        RID t_RID;
        memcpy(&t_RID, (char *) leafPage + RIDList_offset + (i * sizeof(RID)), sizeof(RID));
        if (t_RID.pageNum == rid.pageNum && t_RID.slotNum == rid.slotNum) //compare the RID in the List of RIDs
        {
            memmove((char *) leafPage + RIDList_offset + (i * sizeof(RID)),
                    (char *) leafPage + RIDList_offset + ((i + 1) * sizeof(RID)),
                    freeSpace_ptr - (RIDList_offset + ((i + 1) * sizeof(RID))));
            RIDlist_size--;
            freeSpace_ptr -= sizeof(RID);
            if(RIDlist_size > 0)    //4. Check if any RID list exists
            {
                memcpy((char *) leafPage + key_offset + key_len, &RIDlist_size, sizeof(unsigned short));
            }
            else    //4b if no RIDs; remove the key and shift subsequent records
            {
                memmove((char *) leafPage + key_offset, (char *) leafPage + key_offset + key_len + sizeof(RIDlist_size),
                        freeSpace_ptr - (key_offset + key_len + sizeof(RIDlist_size)));
                freeSpace_ptr -= (key_len + sizeof(RIDlist_size));
                
                unsigned short numKeys;
                
                memcpy(&numKeys, (char *) leafPage + (PAGE_SIZE - sizeof(NodeType) - (sizeof(unsigned short) * 2)), sizeof(unsigned short));
                numKeys--;
                setNumKeys(numKeys, leafPage);
            }
            
            memcpy((char *) leafPage + (PAGE_SIZE - sizeof(NodeType) - sizeof(unsigned short)), &freeSpace_ptr, sizeof(unsigned short)); //5. set free space pointer
            return ixfileHandle.writePage(num_leafPage, leafPage);;
        }
    }
    
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    if (!ixfileHandle.isOpen()) return -1;
    ix_ScanIterator.initialize(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    void *new_page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, new_page);
    NodeType type;
    memcpy(&type, (char *) new_page + (PAGE_SIZE - sizeof(NodeType)), sizeof(NodeType)); //check for nodetype-> leaf/Internal node
    if (type == TypeLeaf)
    {
        leaf_print(attribute, ixfileHandle, 0);
    }
    else
    {
        internalNode_print(attribute, ixfileHandle, 0);
    }
    free(new_page);
}

void IndexManager::internalNode_print(const Attribute &attribute, IXFileHandle &ixFileHandle, PageNum PG_IN) const
{
    /*
     * 1. get keys and print them according to their data type until freeSpacePtr is reached
     * 2. Keep all related pageIDs in a list.
     * 3. check for each pageID if it is a leaf/non-leaf and calls respective functions recursively.
     */
    vector<PageNum> pg_No;
    
    unsigned short freeSpace_ptr;
    
    void *data = malloc(PAGE_SIZE);
    ixFileHandle.readPage(PG_IN, data);
    int offset = 0;
    unsigned short num_ofKeys = 0;
    
    memcpy(&freeSpace_ptr, (char *) data + (PAGE_SIZE - sizeof(NodeType) - sizeof(unsigned short)), sizeof(unsigned short)); // get free space ptr on the page
    
    memcpy(&num_ofKeys, (char *) data + (PAGE_SIZE - sizeof(NodeType) - (sizeof(unsigned short) * 2)), sizeof(unsigned short)); // get number of keys in the page
    PageNum pg = 0;
    memcpy(&pg, (char *) data + offset, sizeof(PageNum));
    pg_No.push_back(pg);
    offset += sizeof(PageNum);
    Attribute attr = attribute;
    cout << "{\n\"keys\": [";
    while (offset < freeSpace_ptr)
    {
        switch (attr.type) {
            case TypeInt: {
                int intKeyVal = 0;
                memcpy(&intKeyVal, (char *) data + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&pg, (char *) data + offset, sizeof(PageNum));
                offset += sizeof(PageNum);
                cout << "\"" << intKeyVal << "\"";
                if (offset < freeSpace_ptr)
                    cout << ",";
                pg_No.push_back(pg);
            }
                break;
                
            case TypeReal: {
                float val = 0;
                memcpy(&val, (char *) data + offset, sizeof(float));
                offset += sizeof(float);
                memcpy(&pg, (char *) data + offset, sizeof(PageNum));
                offset += sizeof(PageNum);
                cout << "\"" << val << "\"";
                if (offset < freeSpace_ptr)
                    cout << ",";
                pg_No.push_back(pg);
            }
                break;
                
            case TypeVarChar: {
                int length = 0;
                unsigned short recordsInKey = 0;
                memcpy(&length, (char *) data + offset, sizeof(int));
                offset += sizeof(int);
                char val[length + 1];
                memcpy(val, (char *) data + offset, (size_t) length);
                val[length] = '\0';
                offset += length;
                memcpy(&pg, (char *) data + offset, sizeof(PageNum));
                offset += sizeof(PageNum);
                cout << "\"" << val << "\"";
                if (offset < freeSpace_ptr)
                    cout << ",";
                pg_No.push_back(pg);
            }
                break;
        }
    }
    cout << "],";
    cout << "\n\"children\":[";
    
    for (int x = 0; x < num_ofKeys + 1; x++) //to check for all the pages associated with node (current page)
    {
        void *dataP = malloc(PAGE_SIZE);
        ixFileHandle.readPage(pg_No[x], dataP);
        NodeType type;
        memcpy(&type, (char *) dataP + (PAGE_SIZE - sizeof(NodeType)), sizeof(NodeType)); //get node type and recursive calls to subsequent nodes
        
        if (type == TypeLeaf)
        {
            leaf_print(attribute, ixFileHandle, pg_No[x]);
        } else
        {
            internalNode_print(attribute, ixFileHandle, pg_No[x]);
        }
        if(x < num_ofKeys)
            cout << ",";
        free(dataP);
    }
    cout << "\n]\n}\n";
    free(data);
}

void IndexManager::leaf_print(const Attribute &attribute, IXFileHandle &ixFileHandle, PageNum PG_IN) const
{
    /*
     * same as non-leaf but no Page ID list to be maintained
     */
    void *data = malloc(PAGE_SIZE);
    ixFileHandle.readPage(PG_IN, data);
    int offset = 0;
    RID rid;
    unsigned short freeSpace_ptr;
    
    memcpy(&freeSpace_ptr, (char *) data + (PAGE_SIZE - sizeof(NodeType) - sizeof(unsigned short)), sizeof(unsigned short));
    cout << "{\n\t\"keys\": [";
    Attribute attr = attribute;
    while (offset < freeSpace_ptr) {
        switch (attr.type) {
            case TypeInt:
            {
                unsigned short records_inKey = 0;
                int key_val;
                memcpy(&key_val, (char *) data + offset, sizeof(int));
                offset += sizeof(int);
                cout << "\"" << key_val << ":[";
                memcpy(&records_inKey, (char *) data + offset, sizeof(unsigned short));
                offset += sizeof(unsigned short);
                
                for (int x = 0; x < records_inKey; x++) {
                    memcpy(&rid.pageNum, (char *) data + offset, sizeof(unsigned));
                    offset += sizeof(unsigned);
                    memcpy(&rid.slotNum, (char *) data + offset, sizeof(unsigned));
                    offset += sizeof(unsigned);
                    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
                    if (x < records_inKey - 1)
                        cout << ",";
                }
                cout << "]\"";
                if (offset < freeSpace_ptr)
                    cout << ",";
                break;
            }
            case TypeReal:
            {
                unsigned short records_inKey = 0;
                float val = 0;
                memcpy(&val, (char *) data + offset, sizeof(float));
                offset = offset + sizeof(float);
                cout << "\"" << val << ":[";
                memcpy(&records_inKey, (char *) data + offset, sizeof(unsigned short));
                offset += sizeof(unsigned short);
                
                for (int x = 0; x < records_inKey; x++) {
                    memcpy(&rid.pageNum, (char *) data + offset, sizeof(unsigned));
                    offset += sizeof(unsigned);
                    memcpy(&rid.slotNum, (char *) data + offset, sizeof(unsigned));
                    offset += sizeof(unsigned);
                    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
                    if (x < records_inKey - 1)
                        cout << ",";
                }
                cout << "]\"";
                if (offset < freeSpace_ptr)
                    cout << ",";
                break;
            }
            case TypeVarChar:
            {
                unsigned short records_inKey = 0;
                int length = 0;
                memcpy(&length, (char *) data + offset, sizeof(int));
                offset += sizeof(int);
                char val[length + 1];
                memcpy(val, (char *) data + offset, (size_t) length);
                val[length] = '\0';
                offset += length;
                cout << "\"" << val << ":[";
                memcpy(&records_inKey, (char *) data + offset, sizeof(unsigned short));
                offset += sizeof(unsigned short);
                for (int x = 0; x < records_inKey; x++) {
                    memcpy(&rid.pageNum, (char *) data + offset, sizeof(unsigned));
                    offset += sizeof(unsigned);
                    memcpy(&rid.slotNum, (char *) data + offset, sizeof(unsigned));
                    offset += sizeof(unsigned);
                    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
                    if (x < records_inKey - 1)
                        cout << ",";
                }
                cout << "]\"";
                if (offset < freeSpace_ptr)
                    cout << ",";
                break;
            }
        }
    }
    cout << "]\n}";
    free(data);
}
void
IndexManager::findLeafForKey(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, PageNum &pageNum, void *page) {
    fileHandle.readPage(pageNum, page);
    NodeType type;
    //getNodeType
    memcpy(&type, (char *) page + (PAGE_SIZE - Node_Type_size), Node_Type_size);
    if (type == TypeLeaf)
        return;

    int keyOffset;
    while (type == TypeNonLeaf) {
        RC rc = fetchKeyOffsetInPage(page, attribute, key, keyOffset);
        if (rc == 0) {
            int offsetKeyLen = getKeyLen(attribute, (char *) page + keyOffset);
            memcpy(&pageNum, (char *) page + keyOffset + offsetKeyLen, size_pagenum);
        } else
            memcpy(&pageNum, (char *) page + keyOffset - size_pagenum, size_pagenum);

        fileHandle.readPage(pageNum, page);
        //getNodeType
        memcpy(&type, (char *) page + (PAGE_SIZE - Node_Type_size), Node_Type_size);
    }
}



RC IndexManager::fetchKeyOffsetInPage(const void *page, const Attribute &attribute, const void *key, int &keyOffset) {
    NodeType type;
    //getNodeType;
    memcpy(&type, (char *) page + (PAGE_SIZE - Node_Type_size), Node_Type_size);
    bool isLeaf = type == TypeLeaf;
    keyOffset = isLeaf ? 0 : size_pagenum;
    int keyLen = getKeyLen(attribute, key);

    unsigned short numKeys, freeSpacePointer;
    //getFreeSpacePointer
    memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
    getNumKeys(numKeys, page);
    if (numKeys == 0 || key == NULL) {
        return -1; // blank node
    }
    for (int i = 0; i < numKeys; i++) {
        RC rc = compareKeyAtOffset(attribute, key, keyOffset, page);
        if (rc == 0) return 0; // found key

        if (rc < 0) { //move to next key
            int tempKeyLen = getKeyLen(attribute, (char *) page + keyOffset);
            if (isLeaf) {
                unsigned short listSize;
                memcpy(&listSize, (char *) page + keyOffset + tempKeyLen, Bit_size2);
                keyOffset += (tempKeyLen + Bit_size2 + listSize * sizeof(RID));
            } else {
                keyOffset += (tempKeyLen + size_pagenum);
            }
        }

        if (rc > 0) {
            //key doesn't exist
            return -2;
        }
    }
    return -3; //end of file
}



int IndexManager::getKeyLen(const Attribute &attribute, const void *key) {
    switch (attribute.type) {
        case TypeVarChar: {
            int len;
            memcpy(&len, key, Bit_size_int);
            return (len + Bit_size_int);
        }
        case TypeReal: {
            return Bit_size_float;
        }
        default: {
            return Bit_size_int;
        }
    }
}



int IndexManager::compareKeyAtOffset(const Attribute &attribute, const void *key, const int offset, const void *page) {
    if (key == NULL) {
        return 0;
    }
    switch (attribute.type) {
        case TypeInt: {
            int tempKey, keyVal;
            memcpy(&tempKey, (char *) page + offset, Bit_size_int);
            memcpy(&keyVal, key, Bit_size_int);
            if (tempKey == keyVal) return 0;
            if (tempKey < keyVal) return -1;
            if (tempKey > keyVal) return 1;
        }
        case TypeReal: {
            float tempKey, keyVal;
            memcpy(&tempKey, (char *) page + offset, Bit_size_float);
            memcpy(&keyVal, key, Bit_size_float);
            if (tempKey == keyVal) return 0;
            if (tempKey < keyVal) return -1;
            if (tempKey > keyVal) return 1;
        }
        case TypeVarChar: {
            int keyLen = 0;
            memcpy(&keyLen, (char *) key, Bit_size_int);
            //Get actual value of varchar
            char keyVal[keyLen + 1];
            memcpy(keyVal, (char *) key + Bit_size_int, (size_t) keyLen);
            keyVal[keyLen] = '\0';

            int tempLen = 0;
            memcpy(&tempLen, (char *) page + offset, Bit_size_int);
            //Get actual value of varchar
            char tempVal[tempLen + 1];
            memcpy(tempVal, (char *) page + offset + Bit_size_int, (size_t) tempLen);
            tempVal[tempLen] = '\0';

            return strcmp(tempVal, keyVal);
        }
    }
    return false;
}

RC
IndexManager::insertInPage(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid,
                           const void *page,
                           PageNum pageNum, void *&splitKey, PageNum &splitPageNum) {
    NodeType type;
    //getNodeType
    memcpy(&type, (char *) page + (PAGE_SIZE - Node_Type_size), Node_Type_size);
    if (type == TypeLeaf) {
        unsigned short freeSpacePointer;
        //getFreeSpacePointer
        memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
        int keyLen = getKeyLen(attribute, key);
        if (freeSpacePointer + keyLen + Bit_size2 + sizeof(RID) < (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum)) { //leaf has space
          

            int keyOffset, keyLen = getKeyLen(attribute, key);
                unsigned short freeSpacePointer;
                RC rc = fetchKeyOffsetInPage(page, attribute, key, keyOffset);
                if (rc != 0) { // Key not found. insert key
                    //getFreeSpacePointer
                    memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                    memmove((char *) page + keyOffset + keyLen + Bit_size2,
                            (char *) page + keyOffset,
                            (size_t) (freeSpacePointer - keyOffset));
                    


                    int keyLen = getKeyLen(attribute, key);
                       unsigned short numKeys, freeSpacePointer;
                       getNumKeys(numKeys, page);
                       //getFreeSpacePointer;
                       memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                       memcpy((char *) page + keyOffset, key, (size_t) keyLen);
                       unsigned short listSize = 0;
                       memcpy((char *) page + keyOffset + keyLen, &listSize, (size_t) Bit_size2);
                       numKeys++;
                       freeSpacePointer += keyLen + Bit_size2;
                       //setFreeSpacePointer
                       memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
                       setNumKeys(numKeys, page);

                }
                unsigned short listSize;
                memcpy(&listSize, (char *) page + keyOffset + keyLen, Bit_size2);
                listSize++;
                memcpy((char *) page + keyOffset + keyLen, &listSize, Bit_size2);

                //getFreeSpacePointer
                memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                memmove((char *) page + keyOffset + keyLen + Bit_size2 + listSize * sizeof(RID),
                        (char *) page + keyOffset + keyLen + Bit_size2 + (listSize - 1) * sizeof(RID),
                        freeSpacePointer - (keyOffset + keyLen + Bit_size2 + (listSize - 1) * sizeof(RID)));

                memcpy((char *) page + keyOffset + keyLen + sizeof(listSize) + (listSize - 1) * sizeof(RID), &rid,
                       sizeof(RID));
                freeSpacePointer += sizeof(RID);
               // setFreeSpacePointer
                memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);





            return fileHandle.writePage(pageNum, page);
        } else { // no space. need to split
            void *newPage = initialize_page;
           
            NodeType type = TypeLeaf;
            unsigned short freeSpace = 0;
            unsigned short nKeys = 0;
            PageNum nextLeaf = 0;

            memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size), &type, Node_Type_size);
            memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpace, Bit_size2);
            memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2)), &nKeys, Bit_size2);
            memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum), &nextLeaf, size_pagenum);
            if (splitLeaf(page, newPage, attribute, key) == 0) { // insert in new leaf
                


                int keyOffset, keyLen = getKeyLen(attribute, key);
                    unsigned short freeSpacePointer;
                    RC rc = fetchKeyOffsetInPage(newPage, attribute, key, keyOffset);
                    if (rc != 0) { // Key not found. insert key
                        //getFreeSpacePointer
                        memcpy(&freeSpacePointer, (char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                        memmove((char *) newPage + keyOffset + keyLen + Bit_size2,
                                (char *) newPage + keyOffset,
                                (size_t) (freeSpacePointer - keyOffset));
                       


                        int keyLen = getKeyLen(attribute, key);
                           unsigned short numKeys, freeSpacePointer;
                           getNumKeys(numKeys, newPage);
                           //getFreeSpacePointer;
                           memcpy(&freeSpacePointer, (char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                           memcpy((char *) newPage + keyOffset, key, (size_t) keyLen);
                           unsigned short listSize = 0;
                           memcpy((char *) newPage + keyOffset + keyLen, &listSize, (size_t) Bit_size2);
                           numKeys++;
                           freeSpacePointer += keyLen + Bit_size2;
                           //setFreeSpacePointer
                           memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
                           setNumKeys(numKeys, newPage);

                    }
                    unsigned short listSize;
                    memcpy(&listSize, (char *) newPage + keyOffset + keyLen, Bit_size2);
                    listSize++;
                    memcpy((char *) newPage + keyOffset + keyLen, &listSize, Bit_size2);

                    //getFreeSpacePointer
                    memcpy(&freeSpacePointer, (char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                    memmove((char *) newPage + keyOffset + keyLen + Bit_size2 + listSize * sizeof(RID),
                            (char *) newPage + keyOffset + keyLen + Bit_size2 + (listSize - 1) * sizeof(RID),
                            freeSpacePointer - (keyOffset + keyLen + Bit_size2 + (listSize - 1) * sizeof(RID)));

                    memcpy((char *) newPage + keyOffset + keyLen + sizeof(listSize) + (listSize - 1) * sizeof(RID), &rid,
                           sizeof(RID));
                    freeSpacePointer += sizeof(RID);
                   // setFreeSpacePointer
                    memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);



            } else { // insert in oldLeaf
            	int keyOffset, keyLen = getKeyLen(attribute, key);
            	    unsigned short freeSpacePointer;
            	    RC rc = fetchKeyOffsetInPage(page, attribute, key, keyOffset);
            	    if (rc != 0) { // Key not found. insert key
            	        //getFreeSpacePointer
            	        memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
            	        memmove((char *) page + keyOffset + keyLen + Bit_size2,
            	                (char *) page + keyOffset,
            	                (size_t) (freeSpacePointer - keyOffset));
            	        


            	        int keyLen = getKeyLen(attribute, key);
            	           unsigned short numKeys, freeSpacePointer;
            	           getNumKeys(numKeys, page);
            	           //getFreeSpacePointer;
            	           memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
            	           memcpy((char *) page + keyOffset, key, (size_t) keyLen);
            	           unsigned short listSize = 0;
            	           memcpy((char *) page + keyOffset + keyLen, &listSize, (size_t) Bit_size2);
            	           numKeys++;
            	           freeSpacePointer += keyLen + Bit_size2;
            	           //setFreeSpacePointer
            	           memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
            	           setNumKeys(numKeys, page);

            	    }
            	    unsigned short listSize;
            	    memcpy(&listSize, (char *) page + keyOffset + keyLen, Bit_size2);
            	    listSize++;
            	    memcpy((char *) page + keyOffset + keyLen, &listSize, Bit_size2);

            	    //getFreeSpacePointer
            	    memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
            	    memmove((char *) page + keyOffset + keyLen + Bit_size2 + listSize * sizeof(RID),
            	            (char *) page + keyOffset + keyLen + Bit_size2 + (listSize - 1) * sizeof(RID),
            	            freeSpacePointer - (keyOffset + keyLen + Bit_size2 + (listSize - 1) * sizeof(RID)));

            	    memcpy((char *) page + keyOffset + keyLen + sizeof(listSize) + (listSize - 1) * sizeof(RID), &rid,
            	           sizeof(RID));
            	    freeSpacePointer += sizeof(RID);
            	   // setFreeSpacePointer
            	    memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
            }

            splitPageNum = (PageNum) fileHandle.getNumberOfPages();
            memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum), (char *) page + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum), size_pagenum);
            memcpy((char *) page + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum), &splitPageNum, size_pagenum);

            int splitKeyLen = getKeyLen(attribute, newPage);
            splitKey = malloc((size_t) splitKeyLen);
            memcpy(splitKey, newPage, (size_t) splitKeyLen);
            fileHandle.writePage(pageNum, page);
            fileHandle.appendPage(newPage);
            return 1;
        }
    } else {
        int keyOffset;
        PageNum childPageNum;
        void *childPage = initialize_page;
        RC fetchRC = fetchKeyOffsetInPage(page, attribute, key, keyOffset);
        if (fetchRC == 0) {
            int keyLen = getKeyLen(attribute, (char *) page + keyOffset);
            memcpy(&childPageNum, (char *) page + keyOffset + keyLen, size_pagenum);
        } else {
            memcpy(&childPageNum, (char *) page + keyOffset - size_pagenum, size_pagenum);
        }

        fileHandle.readPage(childPageNum, childPage);
        RC rc = insertInPage(fileHandle, attribute, key, rid, childPage, childPageNum, splitKey, splitPageNum);
        if (rc != 0) {
            unsigned short freeSpacePointer;
            //getFreeSpacePointer
            memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
            int keyLen = getKeyLen(attribute, splitKey);

            if (freeSpacePointer + keyLen + size_pagenum < (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2))) { // leaf has space
              


                int keyOffset, keyLen = getKeyLen(attribute, splitKey);
                   unsigned short freeSpacePointer, numKeys;
                  // getFreeSpacePointer
                   memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                   getNumKeys(numKeys, page);
                   RC rc = fetchKeyOffsetInPage(page, attribute, splitKey, keyOffset);
                   if (rc != -3) {
                       // not at EOF
                       memmove((char *) page + keyOffset + keyLen + size_pagenum, (char *) page + keyOffset,
                               (size_t) (freeSpacePointer - keyOffset));
                   }
                   memcpy((char *) page + keyOffset, splitKey, (size_t) keyLen);
                   memcpy((char *) page + keyOffset + keyLen, &splitPageNum, size_pagenum);

                   freeSpacePointer += keyLen + size_pagenum;
                   numKeys++;

                   //setFreeSpacePointer
                   memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
                   setNumKeys(numKeys, page);










                fileHandle.writePage(pageNum, page);
            } else { // no free space. split non leaf node
                void *newPage = initialize_page;
            
                NodeType type = TypeNonLeaf;
                 unsigned short FSPointer = 0;
                 unsigned short nKeys = 0;

                 memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size), &type, Node_Type_size);
                 memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &FSPointer, Bit_size2);
                 memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2)), &nKeys, Bit_size2);
                unsigned short newFSP = size_pagenum, newNumKeys = 0;
                //setFreeSpacePointer
                memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &newFSP, Bit_size2);
                setNumKeys(newNumKeys, newPage);

                if (splitNonLeaf(page, newPage, attribute, splitKey) == 0) { // insert in new leaf
                    doInsertNonLeaf(newPage, attribute, splitKey, splitPageNum);

                } else { // insert in oldLeaf
                    doInsertNonLeaf(page, attribute, splitKey, splitPageNum);
                }
                //getFreeSpacePointer
                memcpy(&newFSP, (char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
                getNumKeys(newNumKeys, newPage);

                //get 1st key from new Page;
                int splitKeyLen = getKeyLen(attribute, (char *) newPage + size_pagenum);
                splitKey = malloc((size_t) splitKeyLen);
                newFSP -= (size_pagenum + splitKeyLen);
                newNumKeys--;

                memcpy(splitKey, (char *) newPage + size_pagenum, (size_t) splitKeyLen);

                memmove(newPage, (char *) newPage + size_pagenum + splitKeyLen, newFSP);

                setNumKeys(newNumKeys, newPage);
                //setFreeSpacePointer
                memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &newFSP, Bit_size2);
                splitPageNum = (PageNum) fileHandle.getNumberOfPages();

                fileHandle.writePage(pageNum, page);
                fileHandle.appendPage(newPage);
                return 1;
            }
        }

    }
    return 0;
}

RC
IndexManager::splitLeaf(const void *page, void *newPage, const Attribute &attribute, const void *key) {
    RC rc;
    unsigned short numKeys, freeSpacePointer, newFSP, numKeysPg2, splitOffset = 0;
    getNumKeys(numKeys, page);
    //getFreeSpacePointer
    memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
    unsigned short numKeysPg1 = (unsigned short) (numKeys / 2);
    numKeysPg2 = numKeys - numKeysPg1;
    int keyLen;
    unsigned short listSize;
    for (int i = 0; i < numKeysPg1; i++) {
        keyLen = getKeyLen(attribute, (char *) page + splitOffset);
        memcpy(&listSize, (char *) page + splitOffset + keyLen, Bit_size2);
        splitOffset += (keyLen + Bit_size2 + (listSize * sizeof(RID)));
    }
    if (compareKeyAtOffset(attribute, key, splitOffset, page) < 0) {
        rc = 0;
    } else {
        rc = 1;
    }

    memcpy(newPage, (char *) page + splitOffset, (size_t) (freeSpacePointer - splitOffset));

    newFSP = freeSpacePointer - splitOffset;
    freeSpacePointer = splitOffset;
    //setFreeSpacePointer
    memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
    //setFreeSpacePointer
    memcpy((char *) newPage + (PAGE_SIZE - Node_Type_size - Bit_size2), &newFSP, Bit_size2);

    setNumKeys(numKeysPg1, page);
    setNumKeys(numKeysPg2, newPage);
    return rc;
}



void
IndexManager::doInsertNonLeaf(const void *page, const Attribute &attribute, const void *splitKey, const PageNum splitPageNum) {
    int keyOffset, keyLen = getKeyLen(attribute, splitKey);
    unsigned short freeSpacePointer, numKeys;
   // getFreeSpacePointer
    memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
    getNumKeys(numKeys, page);
    RC rc = fetchKeyOffsetInPage(page, attribute, splitKey, keyOffset);
    if (rc != -3) {
        // not at EOF
        memmove((char *) page + keyOffset + keyLen + size_pagenum, (char *) page + keyOffset,
                (size_t) (freeSpacePointer - keyOffset));
    }
    memcpy((char *) page + keyOffset, splitKey, (size_t) keyLen);
    memcpy((char *) page + keyOffset + keyLen, &splitPageNum, size_pagenum);

    freeSpacePointer += keyLen + size_pagenum;
    numKeys++;

    //setFreeSpacePointer
    memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
    setNumKeys(numKeys, page);

}

RC IndexManager::splitNonLeaf(const void *page, void *newpage, const Attribute &attribute, const void *key) {
    unsigned short numKeys, freeSpacePointer;
    getNumKeys(numKeys, page);
   // getFreeSpacePointer
    memcpy(&freeSpacePointer, (char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
    int keyOffset;
    fetchKeyOffsetInPage(page, attribute, key, keyOffset);

    int splitOffset = size_pagenum, keyLen;
    for (int i = 0; i < numKeys / 2; i++) {
        keyLen = getKeyLen(attribute, (char *) page + splitOffset);
        splitOffset += (keyLen + size_pagenum);
    }

    RC rc;
    unsigned short numKeysPg1, numKeysPg2;
    if (numKeys % 2 == 0) {
        if (keyOffset < splitOffset) { //even number of keys
            numKeysPg1 = (unsigned short) (numKeys / 2 - 1);
            rc = 1;
        } else {
            numKeysPg1 = (unsigned short) (numKeys / 2);
            rc = 0;
        }
    } else if (keyOffset > splitOffset) {
        numKeysPg1 = (unsigned short) (numKeys / 2 + 1);
        rc = 0;
    } else {
        numKeysPg1 = (unsigned short) (numKeys / 2);
        rc = 1;
    }
    numKeysPg2 = numKeys - numKeysPg1;

    splitOffset = size_pagenum;
    for (int i = 0; i < numKeysPg1; i++) {
        keyLen = getKeyLen(attribute, (char *) page + splitOffset);
        splitOffset += (keyLen + size_pagenum);
    }

    memcpy((char *) newpage + size_pagenum, (char *) page + splitOffset, (size_t) (freeSpacePointer - splitOffset));

    unsigned short newFSP = (unsigned short) (freeSpacePointer - splitOffset + size_pagenum);
    //setFreeSpacePointer
    memcpy((char *) newpage + (PAGE_SIZE - Node_Type_size - Bit_size2), &newFSP, Bit_size2);
    setNumKeys(numKeysPg2, newpage);

    freeSpacePointer = (unsigned short) splitOffset;
   // setFreeSpacePointer
    memcpy((char *) page + (PAGE_SIZE - Node_Type_size - Bit_size2), &freeSpacePointer, Bit_size2);
    setNumKeys(numKeysPg1, page);

    return rc;
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if (offset == -1) return IX_EOF;
    memcpy(&rid, (char *) leafPage + offset, sizeof(RID));
    memcpy(key, currentKey, (size_t) keyLen);
    offset += sizeof(RID);
    currentRIDListPosition++;
    if (currentRIDListPosition == currentRIDListLen) {
        if (offset < freeSpacePointer) {
            if ((highKey != nullptr) && ((highKeyInclusive &&
                                          ix->compareKeyAtOffset(attribute, highKey, offset, leafPage) > 0) ||
                                         (!highKeyInclusive &&
                                          ix->compareKeyAtOffset(attribute, highKey, offset, leafPage) >= 0))) {
                offset = -1;
                return 0;
            }
        } else {
            memcpy(&leafPageNum, (char *) leafPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum), size_pagenum);
            if (leafPageNum == 0) {
                offset = -1;
                return 0;
            }
            fileHandle->readPage(leafPageNum, leafPage);
            //getFreeSpacePointer
            memcpy(&freeSpacePointer, (char *) leafPage + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
            offset = 0;
        }
        keyLen = ix->getKeyLen(attribute, (char *) leafPage + offset);
        free(currentKey);
        currentKey = malloc((size_t) keyLen);
        memcpy(currentKey, (char *) leafPage + offset, (size_t) keyLen);
        offset += keyLen;
        memcpy(&currentRIDListLen, (char *) leafPage + offset, Bit_size2);
        offset += Bit_size2;
        currentRIDListPosition = 0;
    }
    return 0;
}

RC IX_ScanIterator::close() {
    free(leafPage);
    return 0;
}

void IX_ScanIterator::initialize(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
                                 const void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
    this->fileHandle = &ixfileHandle;
    this->attribute = attribute;
    this->lowKey = lowKey;
    this->highKey = highKey;
    this->lowKeyInclusive = lowKeyInclusive;
    this->highKeyInclusive = highKeyInclusive;

    ix = IndexManager::instance();
    leafPage = initialize_page;
    leafPageNum = 0;

   ix->findLeafForKey(ixfileHandle, attribute, lowKey, leafPageNum, leafPage);
    //getFreeSpacePointer;
    memcpy(&freeSpacePointer, (char *) leafPage + (PAGE_SIZE - Node_Type_size - Bit_size2), Bit_size2);
    currentRIDListPosition = 0;

    if (lowKey == nullptr) {
        offset = 0;
        keyLen = ix->getKeyLen(attribute, leafPage);
        currentKey = malloc((size_t) keyLen);
        memcpy(currentKey, (char *) leafPage, (size_t) keyLen);
        memcpy(&currentRIDListLen, (char *) leafPage + keyLen, Bit_size2);
        offset += (keyLen + Bit_size2);
    } else {
        keyLen = ix->getKeyLen(attribute, lowKey);
        RC rc = ix->fetchKeyOffsetInPage(leafPage, attribute, lowKey, offset);
        do{
            switch (rc) {
                case 0: {//key found
                    if (lowKeyInclusive) {
                        currentKey = malloc((size_t) keyLen);
                        memcpy(currentKey, (char *) leafPage + offset, (size_t) keyLen);
                        memcpy(&currentRIDListLen, (char *) leafPage + offset + keyLen, Bit_size2);
                        offset += (keyLen + Bit_size2);
                    } else {
                        //move to next key
                        memcpy(&currentRIDListLen, (char *) leafPage + offset + keyLen, Bit_size2);
                        offset += (keyLen + Bit_size2 + currentRIDListLen * sizeof(RID));
                        if (offset < freeSpacePointer) {
                            keyLen = ix->getKeyLen(attribute, (char *) leafPage + offset);
                            currentKey = malloc((size_t) keyLen);
                            memcpy(&currentKey, (char *) leafPage + offset, (size_t) keyLen);
                            memcpy(&currentRIDListLen, (char *) leafPage + offset + keyLen, Bit_size2);
                            offset += (keyLen + Bit_size2);
                        } else {

                        }
                    }
                    break;
                }
                case -1: {//page is empty
                    memcpy(&leafPageNum, (char *) leafPage + (PAGE_SIZE - Node_Type_size - (Bit_size2 * 2) - size_pagenum), size_pagenum);
                    if (leafPageNum == 0) {
                        offset = -1;
                        return;
                    }
                    fileHandle->readPage(leafPageNum, leafPage);
                    rc = ix->fetchKeyOffsetInPage(leafPage, attribute, lowKey, offset);
                    break;
                }
                case -2 : {//key not found
                    keyLen = ix->getKeyLen(attribute, (char *) leafPage + offset);
                    currentKey = malloc((size_t) keyLen);
                    memcpy(&currentKey, (char *) leafPage + offset, (size_t) keyLen);
                    memcpy(&currentRIDListLen, (char *) leafPage + offset + keyLen, Bit_size2);
                    offset += (keyLen + Bit_size2);
                }
                case -3:{
                    offset = -1;
                }
            }

        }while(rc == -1);
    }

}


IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data) {
    ixReadPageCounter++;
    return fileHandle.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
    ixWritePageCounter++;
    return fileHandle.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data) {
    ixAppendPageCounter++;
    return fileHandle.appendPage(data);
}

int IXFileHandle::getNumberOfPages() {
    return fileHandle.getNumberOfPages();
}

RC IXFileHandle::isOpen() {
    return fileHandle.isOpen();
}

