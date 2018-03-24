#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <iostream>
#include <stdlib.h>
#include <dirent.h>
#include <cstring>
#include <sys/stat.h>
#include <map>
#include <algorithm>

#define SUCCESS 0
#define FAIL 1
#define STRING_TO_SEARCH 1
#define MIN_NUM_OF_ARGS 2
#define MULTI_THREAD_LEVEL 10
using namespace std;

// ----------------------------------- first level classes -----------------------------------------

class Key1: public k1Base // the directory to search in
{
private:
	string _directory;

public:

	Key1(string directoryToSearchIn): _directory(directoryToSearchIn){ }


	~Key1(){ }

	bool operator<(const k1Base &other) const {
		Key1* otherKey = (Key1*)&other;
		if (this->_directory.compare(otherKey->getDirectory()) >= 0)
		{
			return false;
		}
		return true;
	}

	const string getDirectory () const
	{
		return this->_directory;
	}

	void toString ()
	{
		cout << this->_directory << endl;
	}

};

class Value1: public v1Base // the string to search
{
public:
	Value1(string searchedString): _string(searchedString){	}

	string getString(){
		return this->_string;
	}

	~Value1() {	}

private:
	string _string;
};

// ----------------------------------- second level classes ----------------------------------------


class Key2: public k2Base // 'files' in the path
{
private:
	string _fileName;

public:

	Key2(string directoryToSearchIn): _fileName(directoryToSearchIn){ }


	~Key2(){ }

	bool operator<(const k2Base &other) const {
		Key2* otherKey = (Key2*)&other;

		if (this->_fileName.compare(otherKey->getFileName()) >= 0)
		{
			return false;
		}
		return true;
	}

	const string getFileName () const
	{
		return this->_fileName;
	}

	void toString () const
	{
		cout << "fileName : " << this->_fileName << endl;
	}
};

class Value2: public  v2Base // the number of the occurrences of the filename
{
public:
	Value2(): _occurrence(1){ }

	~Value2 () { }

private:
	int _occurrence;
};

// ----------------------------------- third level classes -----------------------------------------


class Key3: public k3Base
{
private:
	string _fileName;

public:

	Key3(string directoryToSearchIn): _fileName(directoryToSearchIn){ }


	~Key3(){ }

	bool operator<(const k3Base &other) const {
		Key3* otherKey = (Key3*)&other;

		if (this->_fileName.compare(otherKey->getFileName()) >= 0)
		{
			return false;
		}
		return true;
	}

	const string getFileName () const
	{
		return this->_fileName;
	}
};

class Value3: public  v3Base // the string to search
{
public:
	Value3(int num): _occurrence(num){	}

	int getOccurrence(){
		return this->_occurrence;
	}

	~Value3 () { }

private:
	int _occurrence;
};
// -------------------------------------------------------------------------------------------------

class SearchMapReduce: public MapReduceBase
{
public:
	void Map(const k1Base *const key, const v1Base *const val) const
	{
		string directoryName = ((Key1*) key)->getDirectory();
		string stringToSearch = ((Value1*) val)->getString();

		DIR *dir;
		struct dirent *ent;

		map<Key2, Value2>* files = new map<Key2, Value2>();
		dir = opendir( directoryName.c_str());
		if (dir == NULL)
		{
			closedir(dir);
			return;
		}
		while((ent = readdir(dir)) != NULL)
		{
			string currentFile = ent->d_name;
			// If the string we need to search is a substring of the current file name.
			if (currentFile.find(stringToSearch) != std::string::npos)
			{
				Key2* key2 = new Key2(ent->d_name);
				Value2* value2 = new Value2();
				Emit2(key2, value2);
			}
		}
		files->clear();
		delete files;
		closedir(dir);
	}

	void Reduce(const k2Base *const key, const V2_VEC &vals) const
	{
		int counter = vals.size();
		Key3* key3 = new Key3(((Key2*) key)->getFileName());
		Value3* value3 = new Value3(counter);
		Emit3(key3, value3);
	}
};

void deleteInputVec (IN_ITEMS_VEC& input)
{
	for(unsigned int i = 0; i < input.size(); i++){
		delete input[i].first;
		delete input[i].second;
	}
	input.clear();
}

void deleteOutputVec (OUT_ITEMS_VEC& output)
{
	for(unsigned int i = 0; i < output.size(); i++){
		delete output[i].first;
		delete output[i].second;
	}
	output.clear();
}


int main(int argc, char* argv[]){

	if(argc == 1){
		cerr << "Usage: <substring to search> <folders, separated by space>" << endl;
		exit(FAIL);
	}
	if(argc == MIN_NUM_OF_ARGS){
		return SUCCESS;
	}

	SearchMapReduce searchMapReduce;

	string stringToSearch = argv[STRING_TO_SEARCH];
	IN_ITEMS_VEC input;

	for(int i = 2; i < argc; i++){
		struct stat buf;
		stat(argv[i], &buf);
		if(S_ISDIR(buf.st_mode)){ // Invalid path - not a directory or directory doesn't exists.
			Value1* v1 = new Value1(stringToSearch);
			Key1* k1 = new Key1(argv[i]);
			IN_ITEM item = std::make_pair (k1, v1);
			input.push_back(item);
		}
	};
	OUT_ITEMS_VEC output = RunMapReduceFramework(searchMapReduce, input, MULTI_THREAD_LEVEL, true);

	deleteInputVec(input);

	for (std::vector<OUT_ITEM>::iterator it = output.begin(); it != output.end(); it++)
	{
		int num = ((Value3*) it->second)->getOccurrence();
		while(num > 0)
		{
			cout << dynamic_cast<Key3*>(it->first)->getFileName() << " ";
			num --;
		}
	}

	deleteOutputVec(output);

	return SUCCESS;
}