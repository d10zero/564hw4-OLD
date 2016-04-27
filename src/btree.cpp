/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <iostream>
#include <memory>

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	//first, check if the index file exists
	//if the file exists, open it
	//if not, create a new file
	//need to do somethi9ngn different for each 3 types of int, double and string
	std::cout << "input parameters\n";
	std::cout << "relationName: " << relationName << "\n";
	std::cout << "outIndexName: " << outIndexName << "\n";
	std::cout << "bufMgrIn: " << bufMgrIn << "\n";
	std::cout << "attrByteOffset: " << attrByteOffset << "\n";
	std::cout << "attrType: " << attrType << "\n";
	bufMgr = bufMgrIn;
	//attrByteOffset = attrByteOffset;
	attributeType = attrType;
	if (attributeType == 0){
		//0 = interger attribute type
		nodeOccupancy = INTARRAYNONLEAFSIZE;
		leafOccupancy = INTARRAYLEAFSIZE;
	} 
	else if (attributeType == 1){
		//1 = double attribute type
		nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		leafOccupancy = DOUBLEARRAYLEAFSIZE;
	} 
	else if (attributeType == 3){
		//2 = string attribute type
		nodeOccupancy = STRINGARRAYNONLEAFSIZE;
		leafOccupancy = STRINGARRAYLEAFSIZE;	
	}
	else {
		//should never occur
	}
	try {
		file = new BlobFile(outIndexName, false);
		//try to find the file?

	}catch (FileNotFoundException fnfe){
		//create the new file, which is declared in the header btree.h
		file = new BlobFile(outIndexName, true);
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}


//------------------------------------------------------------------------------
// BTreeIndex::compareIndexKey
//--------------------------------------------------------------------------------

int BTreeIndex::compareIndexKey(const void *one, const void *two, bool string1){
	if(attributeType == INTEGER){
		int key1 = *((int *)one);
		int key2 = *((int *)two);
		if(key1 < key2){
			return 1;
		}
		else if(key2 < key1){
			return -1;
		}
		else{
			return 0;
		}
	}
	else if(attributeType == DOUBLE){
		double key1 = *((double *)one);
		double key2 = *((double *)two);
		if (key1 < key2){
			return 1;
		}
		else if (key1 > key2){
			return true;
		}
		else{
			return 0;
		}
	}
	else if(attributeType == STRING){
		std::string key2;
		if(string1){
			key2 = *((std::string *)two);
		}
		else{
			key2 = std::string((char *)two).substr(0, STRINGSIZE);
		}
		std::string key1 = std::string((char *)one).substr(0, STRINGSIZE);
		if(key1 < key2){
			return 1;
		}
		else if(key1 > key2){
			return -1;
		}
		else{
			return 0;
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
{
	if(lowOpParm != GT && lowOpParm != GTE){
		throw BadOpcodesException();
	}
	if(highOpParm != LT && highOpParm != LTE){
		throw BadOpcodesException();
	}
	if(compareIndexKey(lowValParm, highValParm, false) < 0){
		throw BadScanrangeException();
	}
	// if there are no nodes in the B+ tree
	if(rootPageNum == Page::INVALID_NUMBER){
		exit(1);
	}
	// if there is one node in the B+ tree (the root)
	else if(rootPageNum == 2){
		currentPageNum = rootPageNum;
		bufMgr->readPage(file, currentPageNum, currentPage);
		if(attributeType == 0){ //INT
			LeafNodeInt *currentNode = (LeafNodeInt *)currentPage;
			int i;
			for(i = 0; compareIndexKey(currentNode->keyArray + i, lowValParm, false) > 0 && i < INTARRAYLEAFSIZE; i++);
			if(lowOpParm == GT && compareIndexKey(currentNode->keyArray + i, lowValParm, false) == 0){
				throw NoSuchKeyFoundException();
			}
			nextEntry = i;
			bufMgr->unPinPage(file, currentPageNum, false);
		}
		else if(attributeType == 1){ //DOUBLE
			LeafNodeDouble *currentNode = (LeafNodeDouble *)currentPage;
			int i;
			for(i = 0; compareIndexKey(currentNode->keyArray + i, lowValParm, false) > 0 && i < INTARRAYLEAFSIZE; i++);
			if(lowOpParm == GT && compareIndexKey(currentNode->keyArray + i, lowValParm, false) == 0){
				throw NoSuchKeyFoundException();
			}
			nextEntry = i;
			bufMgr->unPinPage(file, currentPageNum, false);
		}
		else if(attributeType == 2){ //STRING
			LeafNodeString *currentNode = (LeafNodeString *)currentPage;
			int i;
			for(i = 0; compareIndexKey(currentNode->keyArray + i, lowValParm, false) > 0 && i < INTARRAYLEAFSIZE; i++);
			if(lowOpParm == GT && compareIndexKey(currentNode->keyArray + i, lowValParm, false) == 0){
				throw NoSuchKeyFoundException();
			}
			nextEntry = i;
			bufMgr->unPinPage(file, currentPageNum, false);
		}

	}
	// there is more than one node in the B+ tree
	else{
		currentPageNum = rootPageNum;
		bool cont = true;
		while(cont){
			bufMgr->readPage(file, currentPageNum, currentPage);
			if(attributeType == 0){ // int
				NonLeafNodeInt *currentNode = (NonLeafNodeInt *)currentPage;
				int i = 0;
				for(i = 0; currentNode->pageNoArray[i+1] != Page::INVALID_NUMBER && i < INTARRAYNONLEAFSIZE && compareIndexKey(currentNode->keyArray + i, lowValParm, false) > 0; i++);

				if(lowOpParm == GT && compareIndexKey(currentNode->keyArray + i, lowValParm, false) == 0){
					i++;
				}
				PageId prevPageNum = currentPageNum;
				currentPageNum = currentNode->pageNoArray[i];
				bufMgr->unPinPage(file, prevPageNum, false);
				if(currentNode->level == 1){ // if level of node in tree = 1
					break; //  does the page need to be unpinned twice in this case? ****************************************
				}
				// TODO: scanLeaf ****************************************************************
				bufMgr->readPage(file, currentPageNum, currentPage);
				LeafNodeInt *currentNode1 = (LeafNodeInt *)currentPage;
				int j;
				for(j = 0; compareIndexKey(currentNode1->keyArray + j, lowValParm, false) > 0 && j < INTARRAYLEAFSIZE; j++);
				if(lowOpParm == GT && compareIndexKey(currentNode1->keyArray + j, lowValParm, false) == 0){
					throw NoSuchKeyFoundException();
				}
				nextEntry = j;
				bufMgr->unPinPage(file, currentPageNum, false);

			}
			else if(attributeType == 1){ // double
				NonLeafNodeDouble *currentNode = (NonLeafNodeDouble *)currentPage;
				int i = 0;
				for(i = 0; currentNode->pageNoArray[i+1] != Page::INVALID_NUMBER && i < DOUBLEARRAYNONLEAFSIZE && compareIndexKey(currentNode->keyArray + i, lowValParm, false) > 0; i++);

				if(lowOpParm == GT && compareIndexKey(currentNode->keyArray + i, lowValParm, false) == 0){
					i++;
				}
				PageId prevPageNum = currentPageNum;
				currentPageNum = currentNode->pageNoArray[i];
				bufMgr->unPinPage(file, prevPageNum, false);
				if(currentNode->level == 1){ // if level of node in tree = 1
					break; //  does the page need to be unpinned twice in this case? ****************************************
				}
				// TODO: scanLeaf ****************************************************************
				bufMgr->readPage(file, currentPageNum, currentPage);
				LeafNodeDouble *currentNode1 = (LeafNodeDouble *)currentPage;
				int j;
				for(j = 0; compareIndexKey(currentNode1->keyArray + j, lowValParm, false) > 0 && j < DOUBLEARRAYLEAFSIZE; j++);
				if(lowOpParm == GT && compareIndexKey(currentNode1->keyArray + j, lowValParm, false) == 0){
					throw NoSuchKeyFoundException();
				}
				nextEntry = j;
				bufMgr->unPinPage(file, currentPageNum, false);
			}
			else if(attributeType == 2){ // string
				NonLeafNodeString *currentNode = (NonLeafNodeString *)currentPage;
				int i = 0;
				for(i = 0; currentNode->pageNoArray[i+1] != Page::INVALID_NUMBER && i < STRINGARRAYNONLEAFSIZE && compareIndexKey(currentNode->keyArray + i, lowValParm, false) > 0; i++);

				if(lowOpParm == GT && compareIndexKey(currentNode->keyArray + i, lowValParm, false) == 0){
					i++;
				}
				PageId prevPageNum = currentPageNum;
				currentPageNum = currentNode->pageNoArray[i];
				bufMgr->unPinPage(file, prevPageNum, false);
				if(currentNode->level == 1){ // if level of node in tree = 1
					break; //  does the page need to be unpinned twice in this case? ****************************************
				}
				// TODO: scanLeaf ****************************************************************
				bufMgr->readPage(file, currentPageNum, currentPage);
				LeafNodeString *currentNode1 = (LeafNodeString *)currentPage;
				int j;
				for(j = 0; compareIndexKey(currentNode1->keyArray + j, lowValParm, false) > 0 && j < STRINGARRAYLEAFSIZE; j++);
				if(lowOpParm == GT && compareIndexKey(currentNode1->keyArray + j, lowValParm, false) == 0){
					throw NoSuchKeyFoundException();
				}
				nextEntry = j;
				bufMgr->unPinPage(file, currentPageNum, false);
			}
		}
	}
}




// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{

}

}
