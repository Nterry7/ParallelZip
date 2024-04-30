#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pzip.h"

static pthread_barrier_t barrier;
int *localCounts;
pthread_mutex_t freqLock;
pthread_mutex_t zipCountLock;

// struct to hold variables for each thread
struct threadData {
        int size;
        int id;
        int startIdx;
        int endIdx;
        int personalZipCount;
        int *totalZippedCharCount;
        int *charFrequency;
        char *inputChars;
        struct zipped_char *zippedChars;
};


/**
 * zipChars() - thread function to zip chars
 *
 * Input:
 * @tData:		  The struct containing the variables for each thread
 *
 * Outputs:
 * @zipped_chars:         The array of zipped_char structs
 * @zipped_chars_count:   The total count of inserted elements into the zippedChars array.
 * @char_frequency[26]:   Total number of occurences
 *
 */
static void *zipChars(void *tData)
{      
        struct threadData *data = (struct threadData *)tData;
        int charCount = 0;
        int count = 0;
        char currentChar = '\0';
        char prevChar = '\0';
        struct zipped_char currentZip;
        struct zipped_char *personalZip = malloc(data->size * sizeof(struct zipped_char));
        int writeIdx = 0;
        
        // find repeated chars and create local zip array
        for (int i = data->startIdx; i < data->endIdx; i++) {
                currentChar = data->inputChars[i];
                if (prevChar == '\0') {
                        prevChar = currentChar;
                        charCount++;
                        continue;
                }
                else if (prevChar == currentChar) {
                        charCount++;
                        continue;
                }
                else {
                        currentZip.character = prevChar;
                        currentZip.occurence = charCount;
                        personalZip[data->personalZipCount] = currentZip;
                        
                        (data->personalZipCount)++;
                        
                        charCount = 1;
                        prevChar = currentChar;
                }
        }

        currentZip.character = prevChar;
        currentZip.occurence = charCount;  
        personalZip[data->personalZipCount] = currentZip;
        (data->personalZipCount)++;
        
        localCounts[data->id] = data->personalZipCount;
        
        pthread_barrier_wait(&barrier);
        
        pthread_mutex_lock(&zipCountLock);
        (*data->totalZippedCharCount) += data->personalZipCount;
        pthread_mutex_unlock(&zipCountLock);
        
        // frequency
        pthread_mutex_lock(&freqLock); 
        for (long i = 0; i < data->personalZipCount; i++) {
                data->charFrequency[(int)personalZip[i].character - (int)'a'] 
                        += personalZip[i].occurence;
        }
        pthread_mutex_unlock(&freqLock);

        // find where to write to zipped_chars
        for (int i = 0; i < data->id; i++) {
                writeIdx += localCounts[i];
        }
        
        // write to zipped_chars
        for (int i = writeIdx; i < writeIdx + localCounts[data->id]; i++) {
                data->zippedChars[i].character = personalZip[count].character;
                data->zippedChars[i].occurence = personalZip[count].occurence;
                count++;
        }
        
        free(personalZip);
        pthread_exit(NULL);
}


/**
 * pzip() - zip an array of characters in parallel
 *
 * Inputs:
 * @n_threads:		   The number of threads to use in pzip
 * @input_chars:	        The input characters (a-z) to be zipped
 * @input_chars_size:	   The number of characaters in the input file
 *
 * Outputs:
 * @zipped_chars:       The array of zipped_char structs
 * @zipped_chars_count:   The total count of inserted elements into the zippedChars array.
 * @char_frequency[26]: Total number of occurences
 *
 * NOTE: All outputs are already allocated. DO NOT MALLOC or REASSIGN THEM !!!
 *
 */
void pzip(int n_threads, char *input_chars, int input_chars_size,
	  struct zipped_char *zipped_chars, int *zipped_chars_count,
	  int *char_frequency)
{       
        int createStatus = -1;
        int joinStatus = -1;
        int offset = input_chars_size / n_threads;
        localCounts = malloc(n_threads * sizeof(int));
        struct threadData *dataStructs = malloc(n_threads * sizeof(struct threadData));
        struct threadData data;
                
        pthread_t *threads = malloc(n_threads * sizeof(pthread_t));
        if (threads == NULL){
                fprintf(stderr, "Failed to malloc threads\n");
                exit(EXIT_FAILURE);
        }
        
        // init the barrier
        if (pthread_barrier_init(&barrier, NULL, n_threads) != 0) {
                fprintf(stderr, "Failed to init barrier\n");
                exit(EXIT_FAILURE);
        }

        // init mutexes
        if(pthread_mutex_init(&zipCountLock, NULL) != 0) {
                fprintf(stderr, "Failed to init zipCountLock\n");
                exit(EXIT_FAILURE);
        }

        if(pthread_mutex_init(&freqLock,NULL) != 0) {
                fprintf(stderr, "Failed to init freqLock");
                exit(EXIT_FAILURE);
        }

        // create threads
        for (int i = 0; i < n_threads; i++) {
                data.size = input_chars_size / n_threads;
                data.id = i;
                data.startIdx = offset * i;
                data.endIdx = offset * i + offset;
                data.charFrequency = char_frequency;
                data.personalZipCount = 0;
                data.inputChars = input_chars;
                data.zippedChars = zipped_chars;
                data.totalZippedCharCount = zipped_chars_count;
                dataStructs[i] = data;
                
                createStatus = pthread_create(&threads[i], NULL, zipChars, &dataStructs[i]);
                if (createStatus != 0) {
                        fprintf(stderr, "Failed to create thread %d\n",i);
                        exit(EXIT_FAILURE);
                }
        }

        // join threads
        for (int i = 0; i < n_threads; i++) {
                joinStatus = pthread_join(threads[i], NULL);
                if (joinStatus != 0) { 
                        fprintf(stderr, "Failed to join thread %d\n",i);
                        exit(EXIT_FAILURE);
                }
        }
       
        pthread_mutex_destroy(&freqLock);
        pthread_mutex_destroy(&zipCountLock);
        pthread_barrier_destroy(&barrier);
        free(localCounts);
        free(threads);
        free(dataStructs);
        return;
}
