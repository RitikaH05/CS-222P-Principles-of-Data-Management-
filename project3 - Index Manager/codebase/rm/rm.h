
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() {};

    ~RM_ScanIterator() {};

    void initialize(RBFM_ScanIterator scanIterator);

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);

    RC close();

private:
    RBFM_ScanIterator _scanIterator;
};


// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
private:
    IX_ScanIterator ix_scanIterator;
public:
    RM_IndexScanIterator() {};    // Constructor
    ~RM_IndexScanIterator() {};    // Destructor

    void initialize(IX_ScanIterator scanIterator);

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key);    // Get next matching entry
    RC close();                        // Terminate index scan
};


// Relation Manager
class RelationManager {
public:
    static RelationManager *instance();


    void createRecordDescriptorTable(vector<Attribute> &recordDescriptor);

    void createRecordDescriptorIndex(vector<Attribute> &recordDescriptor);

    void createRecordDescriptorColumns(vector<Attribute> &recordDescriptor);

    int rmgetByteForNullsIndicator(int fieldCount);

    void prepareRecordTable(int fieldCount, unsigned char *nullFieldsIndicator,
                            const int tableNameLength, const int fileNameLength,
                            const string &tableName, const string &fileName, const int id,
                            void *buffer, int *recordSize);

    void prepareRecordColumns(int fieldCount,
                              unsigned char *nullFieldsIndicator, int id, const int columnNameLength,
                              const string &columnName, const int columnType, const int columnLength,
                              const int columnPosition, void *buffer, int *recordSizeColumn);

    void prepareRecordIndex(int fieldCount,
                            unsigned char *nullFieldsIndicator, const int tableNameLength,
                            const int attrNameLength, const string &tableName,
                            const string &attrName, const int indexID, void *buffer,
                            int *recordSizeTable);

    bool alreadyExists(const string &fileName);

    void insertCatalog(const vector<Attribute> recordDescriptor, const int tableNameLength,
                       const int fileNameLength, const string &tableName,
                       const string &fileName, const int idTable,
                       int *recordSizeTable, int *recordSizeColumn, RID &ridTable, RID &ridColumn);

    void updateTableID(int tableID);

    void updateIndexID(int indexID);

    RC getIndexID();

    RC getTableID();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    RC createIndex(const string &tableName, const string &attributeName);

    RC destroyIndex(const string &tableName, const string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const string &tableName,
                 const string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);

    size_t getAttrLen(const Attribute &attribute, const void *key);

// Extra credit work (10 points)
public:
    RC addAttribute(const string &tableName, const Attribute &attr);

    RC dropAttribute(const string &tableName, const string &attributeName);


protected:
    RelationManager();

    ~RelationManager();

private:
    static RelationManager *_rm;
    string fileNameTable = "Tables";
    string fileNameColumn = "Columns";
    string fileNameIndex = "Indices";
    //FileHandle fileHandleTables;
    //FileHandle fileHandleColumns;
    RecordBasedFileManager *rbfm_obj;
    IndexManager *im_obj;

    RC insertIndexInCatalog(const string tableName, const string attributeName);

    void insertIndexEntries(string tableName, const void *record, RID rid);

    void deleteIndexEntries(const string &tableName, void *record, const RID &rid);
};

#endif
