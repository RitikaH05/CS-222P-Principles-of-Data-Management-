#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan


// Node
typedef enum {
    TypeLeaf = 0, TypeNonLeaf = 1
} NodeType;


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    PagedFileManager *pagedFileManager;

public:
    static IndexManager* instance();

    // Create an index file.
    RC createFile(const string &fileName);

    // Delete an index file.
    RC destroyFile(const string &fileName);

    // Open an index and return an ixfileHandle.
    RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

    // Close an ixfileHandle for an index.
    RC closeFile(IXFileHandle &ixfileHandle);

    // Insert an entry into the given index that is indicated by the given ixfileHandle.
    RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixfileHandle.
    RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixfileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
    void leaf_print(const Attribute &attribute, IXFileHandle &ixFileHandle, PageNum pg) const;
    void internalNode_print(const Attribute &attribute, IXFileHandle &ixFileHandle, PageNum PG_IN) const;

    void findLeafForKey(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, PageNum &pageNum,
                        void *page);

    RC fetchKeyOffsetInPage(const void *page, const Attribute &attribute, const void *key, int &keyOffset);

    int compareKeyAtOffset(const Attribute &attribute, const void *key, const int offset, const void *page);

    int getKeyLen(const Attribute &attribute, const void *key);

protected:
    IndexManager();
    ~IndexManager();

private:
    static IndexManager *_index_manager;



    RC insertInPage(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid,
                    const void *page,
                    PageNum pageNum, void *&splitKey, PageNum &splitPageNum);

    RC splitLeaf(const void *page, void *newPage, const Attribute &attribute, const void *key);


    void doInsertNonLeaf(const void *page, const Attribute &attribute, const void *key, const PageNum pageNum);

    RC splitNonLeaf(const void *page, void *newpage, const Attribute &attribute, const void *key);
};


class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    void initialize(IXFileHandle &ixfileHandle,
                    const Attribute &attribute,
                    const void      *lowKey,
                    const void      *highKey,
                    bool			lowKeyInclusive,
                    bool        	highKeyInclusive);

    IXFileHandle *fileHandle;
    Attribute attribute;
    const void *lowKey;
    const void *highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    PageNum leafPageNum;
    void * leafPage;
    int offset;
    void *currentKey;
    int currentRIDListPosition;
    unsigned short currentRIDListLen;
    IndexManager* ix;
    int keyLen;
    unsigned short freeSpacePointer;
};



class IXFileHandle {

public:

    FileHandle fileHandle;

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);


    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    int getNumberOfPages();
    RC isOpen();

};

#endif
