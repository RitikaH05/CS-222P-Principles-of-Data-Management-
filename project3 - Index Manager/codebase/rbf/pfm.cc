#include "pfm.h"


bool alreadyExists(const string &fileName) {
    ifstream file(fileName);
    if (file.good()) {
        file.close();
        return true;
    }
    return false;
}


PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance() {
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
}

RC PagedFileManager::createFile(const string &fileName) {
    if (alreadyExists(fileName)) {
        return -1;
    }
    ofstream outfile(fileName);
    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName) {
    return remove(fileName.c_str());
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return fileHandle.open(fileName);
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.close();
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    file = NULL;
}

FileHandle::~FileHandle() {
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    fseek(file, pageNum * PAGE_SIZE, 0);
    if (fread(data, PAGE_SIZE, 1, file) != 1)
        return -1;
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= getNumberOfPages()) return -1;
    fseek(file, pageNum * PAGE_SIZE, 0);
    if (fwrite(data, PAGE_SIZE, 1, file) != 1)
        return -1;
    fflush(file);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    fseek(file, 0, SEEK_END);
    if (fwrite(data, PAGE_SIZE, 1, file) != 1)
        return -1;
    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    return (unsigned int) (size / PAGE_SIZE);
}

RC FileHandle::collectCounterValues(unsigned &readPageCount,
                                    unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::open(const string &fileName) {
    if (alreadyExists(fileName)) {
        if(isOpen()) return -2;
        file = fopen(fileName.c_str(), "r+b");
        return 0;
    }
    return -1;
}

RC FileHandle::close() {
    return fclose(file);
    file = NULL;
}

RC FileHandle::isOpen() {
    return file != NULL;
}


