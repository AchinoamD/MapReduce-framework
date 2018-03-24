#include <list>
#include "MapReduceFramework.h"
#include <ctime>
#include <map>
#include <sys/time.h>
#include <iostream>
#include <stdlib.h>
#include <semaphore.h>
#include <algorithm>

#define CHUNK_SIZE 10

#define SUCCESS 0
#define FAIL 1
#define ERROR -1

#define EMPTY 0

#define MICRO_TO_NANO 1000
#define SEC_TO_NANO 1000000000

#define OPEN_MESSAGE "RnuMapReduceFramework started with %d threads\n"
#define CREATE_THREAD_MESSAGE "Thread %s created %s\n"
#define TERMINATE_THREAD_MESSAGE "Thread %s terminated %s\n"
#define MAP_AND_SHUFFLE_TIME "Map and shuffle took %f\n"
#define REDUCE_TIME "Reduce took %f\n"
#define FINISH_MESSAGE "RunMapReduceFramework finished\n"

using namespace std;

typedef pair<k2Base*, v2Base*> SecondLevelPairs;
typedef list<SecondLevelPairs> mapOutputPerThread;

typedef list<OUT_ITEM> reduceOutputPerThread;

// ---------------------------------------- Comparator ---------------------------------------------

bool compare(k2Base* k1, k2Base* k2);

// -------------------------------------- Level1 members -------------------------------------------

MapReduceBase* mapReduceBase;
FILE* logFile;
bool toDelete;
int MULTI_THREAD_LEVEL;

IN_ITEMS_VEC inItemsVec;
unsigned int nextValueIndex;

vector<pthread_t>* execMapThreads;
map<pthread_t, mapOutputPerThread*> mapPthreadToContainer;
bool mapThreadsDone;

// -------------------------------------- Level2 members -------------------------------------------

vector<pthread_t>* execReduceThreads;
map<pthread_t, reduceOutputPerThread*> reducePthreadToContainer;

pthread_t shuffleThread;
map<k2Base*, V2_VEC, bool (*) (k2Base*, k2Base*)> shuffleOutput(compare);

// -------------------------------------- Level3 members -------------------------------------------

list<OUT_ITEM> outItemsList;
OUT_ITEMS_VEC outItemsVec;
vector<std::pair<k2Base*, V2_VEC>> vecFromMap;

// ----------------------------------- Mutex and semaphore -----------------------------------------

sem_t availablePairs;

pthread_mutex_t mapPthreadToContainer_mutex;
pthread_mutex_t reducePthreadToContainer_mutex;
pthread_mutex_t nextValueIndex_mutex;
pthread_mutex_t logFile_mutex;

// -------------------------------------- Class functions ------------------------------------------

/**
 * Compare the content of the two given pointers. This function is used to find an element in the shuffleOutput map.
 * @param k1 the first key to compare to.
 * @param k2 the second key to compare with.
 * @return if (*k1) < (*k2) true, and false otherwise.
 */
bool compare(k2Base* k1, k2Base* k2){
	return ((*k1) < (*k2));
}

/**
 * This function delete and clear all the data structures that memoy was allocated for them.
 */
void clearDataStructures()
{
	execMapThreads->clear();
	delete execMapThreads;

	execReduceThreads->clear();
	delete execReduceThreads;

	inItemsVec.clear();
	outItemsList.clear();

	if (toDelete)
	{
		// Delete mapPthreadToContainer.
		for(map<pthread_t, mapOutputPerThread*>::iterator it = mapPthreadToContainer.begin();
		    it != mapPthreadToContainer.end(); ++ it){
			delete it->second;
		}
		mapPthreadToContainer.clear();

		// Delete shuffleOutput.
		for(map<k2Base*, V2_VEC>::iterator it = shuffleOutput.begin();
		    it != shuffleOutput.end(); ++ it){
			for (v2Base* v: it->second) {
				delete v;
			}
			delete it->first;
		}
		shuffleOutput.clear();
		vecFromMap.clear();
	}

	// Delete reducePthreadToContainer.
	for(map<pthread_t, reduceOutputPerThread*>::iterator it = reducePthreadToContainer.begin();
	    it != reducePthreadToContainer.end(); ++ it){
		delete it->second;
	}
	reducePthreadToContainer.clear();
}

/**
 * Comperator to compare between two elements of from the third stage <k3Base*, v3Base*>.
 * @param firstElem the first element to compare with.
 * @param secondElem the second element to compare with.
 * @return true if firstElem is smaller then secondElem, and false overwise.
 */
bool pairCompare(const OUT_ITEM& firstElem, const OUT_ITEM& secondElem)
{
	return *firstElem.first < *secondElem.first;
}

/**
 * Merge and sort the reduce output to the final output.
 * Finally we copy the outItemsList to the final output container - outItemsvec.
 */
void mergeAndSort()
{
	// Merge all the reduce output into one container.
	for (map<pthread_t, reduceOutputPerThread*>::iterator it = reducePthreadToContainer.begin();
	     it != reducePthreadToContainer.end(); ++ it)
	{
		outItemsList.merge(*(it->second));
	}

	// Sort the container.
	outItemsList.sort(pairCompare);

	// Copy the list to a vector (because this is the format of the output).
	copy(outItemsList.begin(), outItemsList.end(), back_inserter(outItemsVec));
}

/**
 * Lock the given mutex.
 * @param funcName the function name where we came from.
 * @param mutex the mutex to lock.
 */
void lock(pthread_mutex_t& mutex)
{
	if(pthread_mutex_lock(&mutex) != SUCCESS)
	{
		cerr << "MapReduceFramework Failure: pthread_mutex_lock failed." << endl;
		exit(FAIL);
	}
}

/**
 * Unlock the given mutex.
 * @param funcName the function name where we came from.
 * @param mutex the mutex to unlock.
 */
void unlock(pthread_mutex_t& mutex)
{
	if(pthread_mutex_unlock(&mutex) != SUCCESS)
	{
		cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed." << endl;
		exit(FAIL);
	}
}

/**
 * The function computes the current time and returns a string that represnt the time.
 * @return a string that represnt the current time, according to what was wirtten in the exercise.
 */
string getTime()
{
	time_t time1;
	struct tm * timeDetails;
	char timeToString [80];

	time (&time1);
	timeDetails = localtime (&time1);

	strftime (timeToString,80,"[%d.%m.%Y %X]",timeDetails);
	return timeToString;
}
/**
 * The function that each execMap thread runs when it created.
 */
void* execMap(void*) {
	lock(mapPthreadToContainer_mutex);
	unlock(mapPthreadToContainer_mutex);

	lock(nextValueIndex_mutex);
	unsigned int save_nextValueIndex = nextValueIndex;
	nextValueIndex += CHUNK_SIZE;
	unlock(nextValueIndex_mutex);
	// While there are more pairs to read from the input, we want the thread to keep read and work
	// on them.
	while (save_nextValueIndex < inItemsVec.size()) {
		for(unsigned int i = save_nextValueIndex; (i < save_nextValueIndex + CHUNK_SIZE) &&
				(i < inItemsVec.size());i++){
			mapReduceBase->Map(inItemsVec[i].first, inItemsVec[i].second);
		}
		lock(nextValueIndex_mutex);
		save_nextValueIndex = nextValueIndex;
		nextValueIndex += CHUNK_SIZE;
		unlock(nextValueIndex_mutex);
	}

	lock(logFile_mutex);
	fprintf(logFile, TERMINATE_THREAD_MESSAGE, "execMap", getTime().c_str());
	unlock(logFile_mutex);
	return nullptr;
}

/**
 * The function that actually runs the shuffle operation.
 */
void shuffleOperation(){
	for(int j = 0; j < MULTI_THREAD_LEVEL; j++){
		while (!mapPthreadToContainer[(*execMapThreads)[j]]->empty())
		{
			// Search the given key in the shuffleOutput list.
			k2Base* key = mapPthreadToContainer[(*execMapThreads)[j]]->front().first;
			map<k2Base*, V2_VEC>::iterator it;
			it = shuffleOutput.find(key);
			if (it != shuffleOutput.end()) // The key is already exists in the shuffle output.
			{
				shuffleOutput[key].push_back(mapPthreadToContainer[
						                             (*execMapThreads)[j]]->front().second);
				if(toDelete){
					delete key;
				}
			}
			else // The key is not exists in the shuffle output.
			{
				V2_VEC newVec;
				newVec.push_back(mapPthreadToContainer[(*execMapThreads)
				[j]]->front().second);
				shuffleOutput[key] = newVec;
			}

			if (sem_wait(&availablePairs) != SUCCESS) // down the semaphore.
			{
				cerr << "MapReduceFramework Failure: sem_wait failed." << endl;
				exit(FAIL);
			}
			mapPthreadToContainer[(*execMapThreads)[j]]->pop_front();
		}
	}
}
/**
 * The goal of the shuffle function is to merge all the pairs with the same key. The Shuffle
 * converts a list of <k2, v2> to a list of <k2, list<v2> >, where each element in this list has a
 * unique key.
 */
void* shuffle(void*)
{
	int semValue;
	if (sem_getvalue(&availablePairs, &semValue) != SUCCESS)
	{
		cerr << "MapReduceFramework Failure: sem_getvalue failed." << endl;
		exit(FAIL);
	}

	while((semValue > 0) || (!mapThreadsDone)) // There are pairs to work on.
	{
		shuffleOperation();
		if (sem_getvalue(&availablePairs, &semValue) != SUCCESS){
			cerr << "MapReduceFramework Fa  ilure: sem_getvalue failed." <<
			                                                             endl;
			exit(FAIL);
		}
	}

	lock(logFile_mutex);
	fprintf(logFile, TERMINATE_THREAD_MESSAGE, "shuffle", getTime().c_str());
	unlock(logFile_mutex);

	return nullptr;
}

/**
 * The function that each execReduce thread runs when it created.
 */
void* execReduce(void*) {
	lock(reducePthreadToContainer_mutex);
	unlock(reducePthreadToContainer_mutex);

	lock(nextValueIndex_mutex);
	unsigned int save_nextValueIndex = nextValueIndex;
	nextValueIndex += CHUNK_SIZE;
	unlock(nextValueIndex_mutex);

	while(save_nextValueIndex < vecFromMap.size()){
		for(unsigned int i= save_nextValueIndex; (i< save_nextValueIndex + CHUNK_SIZE) && (i < vecFromMap.size()); i++){
			mapReduceBase->Reduce(vecFromMap[i].first, vecFromMap[i].second);
		}
		lock(nextValueIndex_mutex);
		save_nextValueIndex = nextValueIndex;
		nextValueIndex += CHUNK_SIZE;
		unlock(nextValueIndex_mutex);
	}
	lock(logFile_mutex);
	fprintf(logFile, TERMINATE_THREAD_MESSAGE, "execReduce", getTime().c_str());
	unlock(logFile_mutex);

	return nullptr;
}

/**
 * Create the execReduce threads.
 */
void createAndJoinExecReduceThreads()
{
	lock(reducePthreadToContainer_mutex);

	execReduceThreads = new vector<pthread_t>();
	// Create the execReduce threads.
	for(int i = 0; i < MULTI_THREAD_LEVEL; i++)
	{
		pthread_t newThread;
		int retVal = pthread_create(&newThread, NULL, execReduce, NULL);
		if (retVal){
			cerr << "MapReduceFramework Failure: pthread_create failed." << endl;
			exit(FAIL);
		}

		lock(logFile_mutex);
		fprintf(logFile, CREATE_THREAD_MESSAGE, "execReduce", getTime().c_str());
		unlock(logFile_mutex);

		execReduceThreads->push_back(newThread);

		// Initialize the reducePthreadToContainer map.
		reduceOutputPerThread* threadContainer = new reduceOutputPerThread();
		reducePthreadToContainer[(*execReduceThreads)[i]] = threadContainer;
	}

	unlock(reducePthreadToContainer_mutex);

	// Waits for all the threads of execReduce to finished.
	for(int i = 0; i < MULTI_THREAD_LEVEL; i++) {
		if (pthread_join((*execReduceThreads)[i], NULL) != SUCCESS)
		{
			cerr << "MapReduceFramework Failure: pthread_join failed." << endl;
			exit(FAIL);
		}
	}
}

/**
 * Create the execMap threads.
 * @param itemsVec the input vector, contains pairs of <k1, v1>.
 */
void createAndJoinExecMapThreads()
{
	lock(mapPthreadToContainer_mutex);
	// Create the execMap threads.
	for(int i = 0; i < MULTI_THREAD_LEVEL; i++)
	{
		pthread_t newThread;
		int retVal = pthread_create(&newThread, NULL, execMap, NULL);
		if (retVal){
			cerr << "MapReduceFramework Failure: pthread_create failed." << endl;
			exit(FAIL);
		}
		lock(logFile_mutex);
		fprintf(logFile, CREATE_THREAD_MESSAGE, "execMap", getTime().c_str());
		unlock(logFile_mutex);

		execMapThreads->push_back(newThread);

		// Initialize the mapPthreadToContainer map.
		mapOutputPerThread* threadContainer = new mapOutputPerThread();
		mapPthreadToContainer[(*execMapThreads)[i]] = threadContainer;
	}
	unlock(mapPthreadToContainer_mutex);

	// Create the Shuffle thread.
	int retVal = pthread_create(&shuffleThread, NULL, shuffle, NULL);
	if (retVal){
		cerr << "MapReduceFramework Failure: pthread_create failed." << endl;
		exit(FAIL);
	}

	lock(logFile_mutex);
	fprintf(logFile, CREATE_THREAD_MESSAGE, "shuffle", getTime().c_str());
	unlock(logFile_mutex);

	// Waits for all the threads of execMap to finished.
	for(int i = 0; i < MULTI_THREAD_LEVEL; i++) {
		if (pthread_join((*execMapThreads)[i], NULL) != SUCCESS){
			cerr << "MapReduceFramework Failure: pthread_join failed." << endl;
			exit(FAIL);
		}
	}

	mapThreadsDone = true;
	if (pthread_join(shuffleThread, NULL) != SUCCESS) {
		cerr << "MapReduceFramework Failure: pthread_join failed." << endl;
		exit(FAIL);
	}
}

/**
 * The main function of th framework that responsible to runs the whole flow.
 * @param mapReduce a referene to the MapReduceBase object of the user.
 * @param itemsVec the input vector, contains <k1, v1> pairs.
 * @param multiThreadLevel the number of threads that will runs the Map / Reduce function in parallel.
 * @param autoDeleteV2K2 indicates if the user responsible to delete the second level key and
 * valyes, or the framework.
 * @return the output vector <k3, v3> pairs.
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
	// Clear and initialize global variables.
	logFile = nullptr;
	inItemsVec.clear();
	mapPthreadToContainer.clear();
	reducePthreadToContainer.clear();
	shuffleOutput.clear();
	outItemsList.clear();
	outItemsVec.clear();
	vecFromMap.clear();

	// initialize variables and mutex
	mapThreadsDone = false;
	nextValueIndex = 0;
	mapReduceBase = &mapReduce;
	inItemsVec = itemsVec;
	toDelete = autoDeleteV2K2;

	MULTI_THREAD_LEVEL = multiThreadLevel;

	execMapThreads = new vector<pthread_t>();

	// initialize mutex
	mapPthreadToContainer_mutex = PTHREAD_MUTEX_INITIALIZER;
	reducePthreadToContainer_mutex = PTHREAD_MUTEX_INITIALIZER;
	nextValueIndex_mutex = PTHREAD_MUTEX_INITIALIZER;
	logFile_mutex = PTHREAD_MUTEX_INITIALIZER;

	if (sem_init(&availablePairs, 0, EMPTY) != SUCCESS)
	{
		cerr << "MapReduceFramework Failure: sem_init failed." << endl;
		exit(FAIL);
	}

	// Create the log file
	logFile = fopen(".MapReduceFramework.log", "a+");
	if(logFile == NULL){
		cerr << "MapReduceFramework Failure: fopen failed." << endl;
		exit(FAIL);
	}

	lock(logFile_mutex);
	fprintf(logFile, OPEN_MESSAGE, multiThreadLevel);
	unlock(logFile_mutex);

	struct timeval start, end;
	if(gettimeofday(&start, NULL) == ERROR){
		cerr << "MapReduceFramework Failure: gettimeofday failed." << endl;
		exit(FAIL);
	}

	createAndJoinExecMapThreads();

	if(gettimeofday(&end, NULL) == ERROR){
		cerr << "MapReduceFramework Failure: gettimeofday failed." << endl;
		exit(FAIL);
	}

	double time = (end.tv_sec - start.tv_sec) * SEC_TO_NANO + (end.tv_usec - start.tv_usec) * MICRO_TO_NANO;
	lock(logFile_mutex);
	fprintf(logFile, MAP_AND_SHUFFLE_TIME, time);
	unlock(logFile_mutex);

	nextValueIndex = 0;

	if(gettimeofday(&start, NULL) == ERROR){
		cerr << "MapReduceFramework Failure: gettimeofday failed." << endl;
		exit(FAIL);
	}

	vector<std::pair<k2Base*, V2_VEC>> temp{shuffleOutput.begin(), shuffleOutput.end()};
	vecFromMap = temp;

	createAndJoinExecReduceThreads();

	// Merge all the reduce containers to the final output container, and sort it.
	mergeAndSort();

	if(gettimeofday(&end, NULL) == ERROR){
		cerr << "MapReduceFramework Failure: gettimeofday failed." << endl;
		exit(FAIL);
	}

	time = (end.tv_sec - start.tv_sec) * SEC_TO_NANO + (end.tv_usec - start.tv_usec) * MICRO_TO_NANO;

	lock(logFile_mutex);
	fprintf(logFile, REDUCE_TIME, time);
	fprintf(logFile, FINISH_MESSAGE);
	unlock(logFile_mutex);

	// Clear and delete all the data bases we used.
	clearDataStructures();

	// Destroy mutexes and semaphore.
	pthread_mutex_destroy(&mapPthreadToContainer_mutex);
	pthread_mutex_destroy(&reducePthreadToContainer_mutex);
	pthread_mutex_destroy(&nextValueIndex_mutex);
	pthread_mutex_destroy(&logFile_mutex);
	sem_destroy(&availablePairs);

	return outItemsVec;
}

/**
 * This function is called by the Map function (implemented by the user) in order to add a new pair
 * of <k2,v2> to the framework's internal data structures.
 * @param key the key to add.
 * @param value the value to add.
 */
void Emit2 (k2Base* key, v2Base* value) {
	SecondLevelPairs pair = make_pair(key, value);
	mapPthreadToContainer[pthread_self()]->push_back(pair);

	if (sem_post(&availablePairs) != SUCCESS) // up the semaphore
	{
		cerr << "MapReduceFramework Failure: sem_post failed." << endl;
		exit(FAIL);
	}
}

/**
 * The Emit3 function is used by the Reduce function in order to add a pair of <k3, v3> to the final
 * output.
 * @param key the key to add.
 * @param value the value to add.
 */
void Emit3 (k3Base* key, v3Base* value) {
	OUT_ITEM pair = make_pair(key, value);
	reducePthreadToContainer[pthread_self()]->push_back(pair);
}

