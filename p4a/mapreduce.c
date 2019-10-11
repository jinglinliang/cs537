#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

#define hash_size 12345
#define value_list_size 1000

//////Data structure//////
typedef struct kv_pair {
	char* key;
	size_t numOfval;
	struct kv_pair * next;
	char* pointerToVal;
	char* value[value_list_size];
}kv_pair;

typedef struct partition_t {
	kv_pair* hash_table[hash_size];
	size_t numOfkv;
	int taken;
}partition_t;


//////Global variable//////
int files_left;
char **arguments; //to get filenames
Mapper mymapper;
Reducer myreducer;

int numOfpartitions;
partition_t *partitions;


pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;


//////MapReduce Functions//////
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long getHashcode(char *key, int size) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % size;
}

int cstring_cmp(const void *a, const void *b) {
    const char **ia = (const char **)a;
    const char **ib = (const char **)b;
    return strcmp(*ia, *ib);
}

char * get_next(char *key, int partition_number){
		int hash_index = (int)getHashcode(key, hash_size);
		kv_pair *pair = partitions[partition_number].hash_table[hash_index];
		while(strcmp(pair->key, key) != 0){
			pair = pair->next;
		}

		char * result = pair->pointerToVal;
		pair->pointerToVal++;
		return result;
}

void * map_wrapper(void * a){
	while (files_left!=0){
		pthread_mutex_lock(&m);

        if(files_left == 0) {
          pthread_mutex_unlock(&m);
          break;
        }

		char *file = arguments[files_left];
		files_left--;
		pthread_mutex_unlock(&m);

		mymapper(file);
	}
	return NULL;
}

void * reduce_wrapper(void * a){
	int partition_index = 0;
	//take one partition to work on
	pthread_mutex_lock(&m);
	while (partitions[partition_index].taken){
		partition_index++;
	}
	partitions[partition_index].taken = 1;
	pthread_mutex_unlock(&m);

	partition_t this_partition = partitions[partition_index];

	//sort
	char* keys[this_partition.numOfkv];
	int hashcode = 0;
	kv_pair *bucket = this_partition.hash_table[hashcode];

	for(int i = 0; i < this_partition.numOfkv; i++){

		//find next non-NUll bucket
		while(bucket == NULL && hashcode<hash_size){
			hashcode++;
			bucket = this_partition.hash_table[hashcode];
		}

		//traverse key chain
		while (bucket!=NULL){
			keys[i] = bucket->key;

			//sort value
			size_t strings_len = bucket->numOfval;
			qsort(bucket->value, strings_len, sizeof(char *), cstring_cmp);
			bucket->value[strings_len] = NULL;

			if (bucket->next!=NULL){
				i++;
			}
			bucket = bucket->next;
		}

		hashcode++;
		bucket = this_partition.hash_table[hashcode];
	}

	size_t strings_len = this_partition.numOfkv;
	qsort(keys, strings_len, sizeof(char *), cstring_cmp);

	//call reducer
	for(int i = 0; i < this_partition.numOfkv; i++){
		myreducer(keys[i], get_next, partition_index);
	}

	return NULL;
}


void MR_Run(int argc, char *argv[],
	    Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers,
	    Partitioner partition) {

	printf("HERE!!!!");
	files_left = argc - 1;
	arguments = argv;
	mymapper = map;
	myreducer = reduce;

	numOfpartitions = num_reducers;
	printf("HERE1!!!!");
	//initialize data structure
	partition_t init_part[numOfpartitions];
	partitions = init_part;
	//partitions = malloc(numOfpartitions * sizeof(struct partition_t));
	for(int i = 0; i < numOfpartitions; i++){
		for(int j = 0; j < hash_size; j++){
			partitions[i].hash_table[j] = NULL;
		}
		partitions[i].numOfkv = 0;
		partitions[i].taken = 0;
	}

	//run mapper
	pthread_t map_threads[num_mappers];
  for(int i = 0; i < num_mappers; i++){
        pthread_create(&map_threads[i], NULL, map_wrapper, NULL);
    }

	for(int i = 0; i < num_mappers; i++){
        pthread_join(map_threads[i],NULL);
    }
	//all mappers done

	//run reducer
	pthread_t reduce_threads[numOfpartitions];
	for(int i = 0; i < numOfpartitions; i++){
        pthread_create(&reduce_threads[i], NULL, reduce_wrapper, NULL);
    }

	for(int i = 0; i < numOfpartitions; i++){
	      pthread_join(reduce_threads[i],NULL);
	  }

}

kv_pair * create_pair (char *key, char *value){
	kv_pair *pair;
	pair = malloc(sizeof(struct kv_pair));
	pair->key = key;
	//pair->value = malloc(value_list_size*sizeof(char*));
	pair->value[0] = value;
	pair->numOfval = 1;
	pair->next = NULL;
	pair->pointerToVal = pair->value[0];
	return pair;
}

void insert_kv (char *key, char *value, int partition_index, int hash_index){
	kv_pair *pair;
	if ((pair=partitions[partition_index].hash_table[hash_index]) != NULL){ //some key exist
		if (strcmp(pair->key, key) == 0) { //same key
			pair->value[pair->numOfval] = value; //TODO value over flow
			pair->numOfval++;
		}
		else {
			//hash chain
			while(pair->next != NULL){
				pair = pair->next;
				if (strcmp(pair->key, key) == 0){
					pair->value[pair->numOfval] = value; //TODO value over flow
					pair->numOfval++;
					break;
				}
			}
			pair->next = create_pair(key,value);
			partitions[partition_index].numOfkv++;
		}
	}
	else {
		pair = create_pair(key, value);
		partitions[partition_index].hash_table[hash_index] = pair;
		partitions[partition_index].numOfkv++;
	}
}

void MR_Emit(char *key, char *value){
	int partition_index = (int)MR_DefaultHashPartition(key, numOfpartitions);
	int hash_index = (int)getHashcode(key, hash_size);

	pthread_mutex_lock(&m);

	insert_kv(key,value,partition_index,hash_index);

	pthread_mutex_unlock(&m);
}
