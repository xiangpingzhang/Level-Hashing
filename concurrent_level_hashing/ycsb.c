#include "level_hashing.h"

/*  YCSB test:
    This is a simple test example to test the concurrent level hashing 
    using a YCSB workload with 50%/50% search/insertion ratio
*/
int main(int argc, char* argv[])
{        
    int thread_num = atoi(argv[1]);             // INPUT: the number of threads

    level_hash *level = level_init(19,thread_num);
    level->thread_num = thread_num;
    uint64_t inserted = 0, queried = 0, t = 0;
    uint8_t key[KEY_LEN];
    uint8_t value[VALUE_LEN];

	FILE *ycsb, *ycsb_read;
	char *buf = NULL;
	size_t len = 0;
    struct timespec start, finish;
    double single_time;

    // if((ycsb = fopen("./workloads/rw-50-50-load.txt","r")) == NULL)
    // {
    //     perror("fail to read");
    // }

    // printf("Load phase begins \n");
	// while(getline(&buf,&len,ycsb) != -1){
	// 	if(strncmp(buf, "INSERT", 6) == 0){
	// 		memcpy(key, buf+7, KEY_LEN-1);
	// 		if (!level_insert(level, key, key))                      
	// 		{
	// 			inserted ++;
	// 		}
	// 		else{
	// 			break;
	// 		}
	// 	}
	// }
	// fclose(ycsb);
    // printf("Load phase finishes: %d items are inserted \n", inserted);

    // if((ycsb_read = fopen("./workloads/rw-50-50-run.txt","r")) == NULL)
    // {
    //     perror("fail to read");
    // }

    thread_queue* run_queue[thread_num];
    int move[thread_num];
    for(t = 0; t < thread_num; t ++){
        run_queue[t] =  calloc(READ_WRITE_NUM/thread_num, sizeof(thread_queue));
        move[t] = 0;
    }

	int operation_num = 0;		
	while(operation_num < READ_WRITE_NUM){
		//memcpy(run_queue[operation_num%thread_num][move[operation_num%thread_num]].key, buf+7, KEY_LEN-1);
		snprintf(run_queue[operation_num%thread_num][move[operation_num%thread_num]].key, KEY_LEN, "%d", operation_num + 1);
		move[operation_num%thread_num] ++;
		operation_num ++;
	}
	// fclose(ycsb_read);
    //Test insert
	sub_thread* insert_thr = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    inserted = 0;
	
	
    clock_gettime(CLOCK_MONOTONIC, &start);	
    for(t = 0; t < thread_num; t++){
        insert_thr[t].id = t;
        insert_thr[t].level = level;
        insert_thr[t].inserted = 0;
        insert_thr[t].run_queue = run_queue[t];
        pthread_create(&insert_thr[t].thread, NULL, (void *)thread_insert, &insert_thr[t]);
    }

    for(t = 0; t < thread_num; t++){
        pthread_join(insert_thr[t].thread, NULL);
    }

	clock_gettime(CLOCK_MONOTONIC, &finish);
    level_statistic(level);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    for(t = 0; t < thread_num; ++t){
        inserted +=  insert_thr[t].inserted;
    }
    printf("Run phase finishes: %ld items are inserted\n", inserted);
    printf("Run phase throughput: %f operations per second \n", READ_WRITE_NUM/single_time);	
    
    //Test Search
    sub_thread* search_thr = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    uint64_t searched = 0;
	
	
    clock_gettime(CLOCK_MONOTONIC, &start);	
    for(t = 0; t < thread_num; t++){
        search_thr[t].id = t;
        search_thr[t].level = level;
        search_thr[t].inserted = 0;
        search_thr[t].run_queue = run_queue[t];
        pthread_create(&search_thr[t].thread, NULL, (void *)thread_search, &search_thr[t]);
    }

    for(t = 0; t < thread_num; t++){
        pthread_join(search_thr[t].thread, NULL);
    }

	clock_gettime(CLOCK_MONOTONIC, &finish);
    level_statistic(level);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    for(t = 0; t < thread_num; ++t){
        searched +=  search_thr[t].inserted;
    }
    
    printf("Run phase finishes: %ld items are searched\n", searched);
    printf("Run phase throughput: %f operations per second \n", READ_WRITE_NUM/single_time);	

    //Test Update
    sub_thread* update_thr = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    uint64_t updated = 0;
	
	
    clock_gettime(CLOCK_MONOTONIC, &start);	
    for(t = 0; t < thread_num; t++){
        update_thr[t].id = t;
        update_thr[t].level = level;
        update_thr[t].inserted = 0;
        update_thr[t].run_queue = run_queue[t];
        pthread_create(&update_thr[t].thread, NULL, (void *)thread_update, &update_thr[t]);
    }

    for(t = 0; t < thread_num; t++){
        pthread_join(update_thr[t].thread, NULL);
    }

	clock_gettime(CLOCK_MONOTONIC, &finish);
    level_statistic(level);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    for(t = 0; t < thread_num; ++t){
        updated +=  update_thr[t].inserted;
    }
    
    printf("Run phase finishes: %ld items are searched\n", updated);
    printf("Run phase throughput: %f operations per second \n", READ_WRITE_NUM/single_time);
    
    //Test Delete
    sub_thread* delete_thr = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    uint64_t deleted = 0;
	
	
    clock_gettime(CLOCK_MONOTONIC, &start);	
    for(t = 0; t < thread_num; t++){
        delete_thr[t].id = t;
        delete_thr[t].level = level;
        delete_thr[t].inserted = 0;
        delete_thr[t].run_queue = run_queue[t];
        pthread_create(&delete_thr[t].thread, NULL, (void *)thread_delete, &delete_thr[t]);
    }

    for(t = 0; t < thread_num; t++){
        pthread_join(delete_thr[t].thread, NULL);
    }

	clock_gettime(CLOCK_MONOTONIC, &finish);
    level_statistic(level);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    for(t = 0; t < thread_num; ++t){
        deleted +=  delete_thr[t].inserted;
    }
    
    printf("Run phase finishes: %ld items are searched\n", deleted);
    printf("Run phase throughput: %f operations per second \n", READ_WRITE_NUM/single_time);

    return 0;
}
