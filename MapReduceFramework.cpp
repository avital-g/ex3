//
// Created by giat on 6/14/21.
//
#include <iostream>
#include <string>
#include <time.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <atomic>
#include <mutex>
#include <unistd.h>
#include <algorithm>
#include "Barrier.h"
#include <queue>

void *print_message_function(void *ptr);

std::mutex get_input_mutex;

typedef struct {
    const MapReduceClient *client;
    const InputVec *inputVec;
    OutputVec *outputVec;
    int multiThreadLevel;
    JobState *jobState;
    std::queue <IntermediateVec *> queue_to_reduce;
    std::mutex read_and_write_to_queue_mutex;
    std::atomic<int> *num_of_tastks_waiting_for_reduction;

} JobContext;

struct ThreadContext {
    std::atomic<int> *next_waiting_pair_index;
    std::atomic<int> *completed_task_counter;
    std::atomic<int> *num_of_threads_still_mapping;

    Barrier *barrier;
//    int* bad_counter;
    JobContext *handle;
    IntermediateVec * intermediateVec;
};
bool sortPairs(const IntermediatePair &x, const IntermediatePair &y)
{
    return x.first < y.first;
}


[[noreturn]] void *oneThreadJob(void *arg) {
    ThreadContext *tc = (ThreadContext *) arg;
    JobContext *jobContext = tc->handle;
    while (tc->next_waiting_pair_index->load() < jobContext->inputVec->size()) {
        // old_value isn't used in this example, but will be
        // in the exercise
        int old_value = (*(tc->next_waiting_pair_index))++;
//        std::lock_guard<std::mutex> guard(get_input_mutex);
        //TODO - fix the way of taking a thread, when to not take, ect.
        const InputPair thisInputPair = (*jobContext->inputVec)[old_value];
        jobContext->client->map(thisInputPair.first, thisInputPair.second, tc);
        (void) old_value;  // ignore not used warning    std::queue<IntermediateVec> queue_to_reduce;
        (*(tc->completed_task_counter))++;
    }
    if(! tc->intermediateVec->empty()){
        std::sort(tc->intermediateVec->begin(), tc->intermediateVec->end(), sortPairs);
    }


//    tc-> num_of_threads_still_mapping--;
    (*(tc->num_of_threads_still_mapping))--;
    std::cout << "one thread done" << std::endl;

//    tc->barrier->barrier();
    //reduce
    while (true) {
        IntermediateVec * vecToReduce = NULL;

        jobContext->read_and_write_to_queue_mutex.lock();
        if (! jobContext->queue_to_reduce.empty()) {

            vecToReduce = jobContext->queue_to_reduce.front();
            jobContext->queue_to_reduce.pop();
        }
        jobContext->read_and_write_to_queue_mutex.unlock();
        if (vecToReduce != NULL){
            jobContext->client->reduce(vecToReduce, tc);
            (*(jobContext->num_of_tastks_waiting_for_reduction))--;
        } else{
            usleep(500);
        }
        // check if anything left in stack, then reduce it
    }
}


std::map<JobHandle, JobState *> jobStateMap;
std::mutex jobStateMapMutex;
std::map<JobHandle, pthread_t> jobExecutorMap;
std::mutex jobExecturorMapMutex;


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->intermediateVec->push_back({key, value});
//    key = tc ->

}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->handle->outputVec->push_back({key, value});
}

bool is_empty(const IntermediateVec *  vec)
{
    return vec->empty();
}

int shuffle_and_send_to_reduction_queue(std::vector<IntermediateVec*> &vector_of_sorted_vectors, JobContext *jobContext) {
    int total_reduce_job= 0;
    int final_size =0;
    for(int i =0 ;i<vector_of_sorted_vectors.size();i++){
        final_size+= vector_of_sorted_vectors[i]->size();
    }
    std:: cout << "inside shuffle - line 116 print"<<std::endl;
    K2 * Kmax;
    while (!vector_of_sorted_vectors.empty()) {
        Kmax= vector_of_sorted_vectors[0]->back().first;
        for (int i = 0; i < vector_of_sorted_vectors.size(); i++) {
            if (  *(Kmax) < *(vector_of_sorted_vectors[i]->back().first) ) {
                Kmax = vector_of_sorted_vectors[i]->back().first;
            }
        }
        IntermediateVec * newVec = new IntermediateVec ;

        for(int i=0;i<vector_of_sorted_vectors.size();i++){
            while ( !((*(vector_of_sorted_vectors[i]->back().first )) < (*Kmax) )) {
//                IntermediatePair * ip = new IntermediatePair(vector_of_sorted_vectors[i]->back()) ;
                IntermediatePair &ip = vector_of_sorted_vectors[i]->back();
                newVec->push_back (ip);
                vector_of_sorted_vectors[i]->pop_back();
                if(vector_of_sorted_vectors[i]->empty()){
                    break;
                }
            }
        }
        std:: cout << "inside shuffle - line 138 print- add to queue"<<std::endl;
        jobContext->read_and_write_to_queue_mutex.lock();
        jobContext->queue_to_reduce.push(newVec);
        (*(jobContext->num_of_tastks_waiting_for_reduction))++;
        jobContext->read_and_write_to_queue_mutex.unlock();

        total_reduce_job++;;
        jobContext->jobState->percentage= jobContext->jobState->percentage + (float)100.*(newVec->size())/total_reduce_job;
        // clean out empty vectors.      (*(jobContext->num_of_tastks_waiting_for_reduction))++;
        vector_of_sorted_vectors.erase(
                std::remove_if(vector_of_sorted_vectors.begin(), vector_of_sorted_vectors.end(), is_empty),
                vector_of_sorted_vectors.end());
//        std::vector<int> to_remove;
//        for (int i = 0; i < vector_of_sorted_vectors.size(); i++) {
//            if (vector_of_sorted_vectors[i]->empty()) {
//                to_remove.push_back(i);
//            }
//        }
//        for(int i = 0; i<to_remove.size();i++) {
//            vector_of_sorted_vectors.erase(vector_of_sorted_vectors.begin() + to_remove[i] - 1);
//
//        }
    }
    return total_reduce_job;
    // TODO - find out if there is another way of adding and removing
}


void *executeJob(void *vPayload) {
    JobContext *job = (JobContext *) vPayload;
    //create thread pool for map
    int multiThreadLevel = job->multiThreadLevel;
    pthread_t thread_list[multiThreadLevel];
    int iterations[multiThreadLevel];
    ThreadContext threadContexts [multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> next_waiting_pair_index(0);
    std::atomic<int> completed_tasks(0);
    std::atomic<int> num_of_threads_still_mapping(multiThreadLevel);

    std::atomic<int> num_of_tasks_for_reduction(0);
    job->num_of_tastks_waiting_for_reduction = &num_of_tasks_for_reduction;
    IntermediateVec intermediateVecList [multiThreadLevel];

    for (int i = 0; i < multiThreadLevel; i++) {
        std::string thread_num = std::to_string(i);
        // TODO insert the correct map funcion with the correct input
        threadContexts[i].next_waiting_pair_index = &next_waiting_pair_index;
        threadContexts[i].num_of_threads_still_mapping = &num_of_threads_still_mapping;
        threadContexts[i].completed_task_counter = &completed_tasks;

        threadContexts[i].handle = job;
        threadContexts[i].intermediateVec = &(intermediateVecList[i]);
        iterations[i] = pthread_create(&thread_list[i], NULL, oneThreadJob, (void *) &threadContexts[i]);
    }
    //map & sort in the threads
    //wait for all threads to finish map & sort
    while (num_of_threads_still_mapping.load() > 0) {
        job->jobState->percentage = (float) 100. * completed_tasks.load() / job->inputVec->size();
//        std::cout << "completed " << completed_tasks.load() << std::endl;
//        std::cout << "mapping threads " << num_of_threads_still_mapping.load() << std::endl;
//        std::cout << "size " << job->inputVec->size() << std::endl;
//        std::cout << "prec" << job->jobState->percentage << std::endl;
        usleep(500);
    }
    job->jobState->percentage = 100;
    //change state
    job->jobState->stage = SHUFFLE_STAGE;
    job->jobState->percentage = 0;

    std:: cout << "line 200 print"<<std::endl;

    std::vector<IntermediateVec *> vectorOfVectors;
    for (int thread_num = 0; thread_num < multiThreadLevel; thread_num++) {
        std:: cout<< sizeof(vectorOfVectors);
        if (!threadContexts[thread_num].intermediateVec->empty()) {
            vectorOfVectors.push_back(threadContexts[thread_num].intermediateVec);
        }
    }
    //sort/shuffle_and_send_to_reduction_queue
    std:: cout << "line 209 print"<<std::endl;
    int total_num_of_pairs=shuffle_and_send_to_reduction_queue(vectorOfVectors, job);
    //change state
    std:: cout << "line 212 print"<<std::endl;
    job->jobState->percentage = 0;
    job->jobState->stage = REDUCE_STAGE;
    while(job->num_of_tastks_waiting_for_reduction->load()>0){
        job->jobState->percentage = (float) 100. * job->outputVec->size()/ total_num_of_pairs;
        usleep(500);
    }
    job->jobState->percentage =100;
    for(int i =0; i< multiThreadLevel; i++){
        pthread_cancel(thread_list[i]);
    }
    return 0;
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    JobContext *thisJob = new JobContext();
    thisJob->client = &client;
    thisJob->outputVec = &outputVec;
    thisJob->inputVec = &inputVec;
    thisJob->multiThreadLevel = multiThreadLevel;

    JobState *jobState = new JobState;
    thisJob->jobState = jobState;
    jobState->stage = UNDEFINED_STAGE;
    jobState->percentage = 0;
//    thisJob->jobState = jobState;

    pthread_t executor;
    pthread_create(&executor, NULL, executeJob, thisJob);

    jobExecutorMap[thisJob] = executor;
    jobStateMap[thisJob] = jobState;
    return thisJob;
}

void waitForJob(JobHandle job) {
    int ret = pthread_join(jobExecutorMap[job], NULL);
    if (ret != 0) {
        std::cout << "system error: text\n";
    }

}

void getJobState(JobHandle job, JobState *state) {
    JobContext *this_job = (JobContext *) job;
    *state = *this_job->jobState;
}

void closeJobHandle(JobHandle job) {
//    pthread_cancel(jobExecutorMap[job]);
    delete (JobContext *) job;
}
