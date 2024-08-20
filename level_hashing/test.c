#include "level_hashing.h"
#include <time.h>
/*  Test:
    This is a simple test example to test the creation, insertion, search, deletion, update in Level hashing
*/
int main(int argc, char* argv[])                        
{
    int level_size = atoi(argv[1]);                     // INPUT: the number of addressable buckets is 2^level_size
    int insert_num = atoi(argv[2]);                     // INPUT: the number of items to be inserted
    
    clock_t start_time,end_time;
    double time_taken;
    double ops;   

    printf("Pre alloc memory:\n");
    //pre-allocate and format key and value arrays
    uint8_t **keysOrValues = (uint8_t**)malloc((insert_num + 1 )* sizeof(uint8_t *));
    for (int i = 0; i < insert_num + 1; i++) {
        keysOrValues[i] = (uint8_t *)malloc(KEY_LEN * sizeof(uint8_t));
    }
    printf("Alloc memory done\n");
    //format keys and values
    for (int i = 1; i < insert_num + 1; i++) {
        snprintf(keysOrValues[i], KEY_LEN, "%d", i + 1);
    }

    level_hash *level = level_init(level_size);
    uint64_t inserted = 0, i = 0;

    start_time = clock();
    for (i = 1; i < insert_num + 1; i ++)
    {
        if (!level_insert(level, keysOrValues[i], keysOrValues[i]))                               
        {
            inserted ++;
        }else
        {
            // printf("Expanding: space utilization & total entries: %f  %ld\n", \
            //     (float)(level->level_item_num[0]+level->level_item_num[1])/(level->total_capacity*ASSOC_NUM), \
            //     level->total_capacity*ASSOC_NUM);
            level_expand(level);
            level_insert(level, keysOrValues[i], keysOrValues[i]);
            inserted ++;
        }
    }
    end_time = clock();
    time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    ops = (double)insert_num / time_taken; 
    printf("%ld items are inserted ! takes %f sec, OPS %f \n", inserted ,time_taken ,ops);

    printf("The static search test begins ...\n");
    start_time = clock();
    for (i = 1; i < insert_num + 1; i ++)
    {
        uint8_t* get_value = level_static_query(level, keysOrValues[i]);
        // if(memcmp(get_value,keysOrValues[i],KEY_LEN) != 0)
        //     printf("Search the key %s: ERROR! \n", keysOrValues[i]);
    }
    end_time = clock();
    time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    ops = (double)insert_num / time_taken; 
    printf("%ld items are static searched ! takes %f sec, OPS %f \n", inserted ,time_taken ,ops);

    printf("The dynamic search test begins ...\n");
    start_time = clock();
    for (i = 1; i < insert_num + 1; i ++)
    {
        uint8_t* get_value = level_dynamic_query(level, keysOrValues[i]);
        // if(memcmp(get_value,keysOrValues[i],KEY_LEN) != 0)
        //     printf("Search the key %s: ERROR! \n", keysOrValues[i]);
    }
    end_time = clock();
    time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    ops = (double)insert_num / time_taken; 
    printf("%ld items are dynamic searched! takes %f sec, OPS %f \n", inserted ,time_taken ,ops);

    printf("The update test begins ...\n");
    start_time = clock();
    for (i = 1; i < insert_num + 1; i ++)
    {
        if(level_update(level, keysOrValues[i], keysOrValues[i])){
            exit(0);
            // printf("Update the value of the key %s: ERROR! \n", keysOrValues[i]);
        }
    }
    end_time = clock();
    time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    ops = (double)insert_num / time_taken; 
    printf("%ld items are updated ! takes %f sec, OPS %f \n", inserted ,time_taken ,ops);

    printf("The deletion test begins ...\n");
    start_time = clock();
    for (i = 1; i < insert_num + 1; i ++)
    {
        if(level_delete(level, keysOrValues[i])){
            //printf("Delete the key %s: ERROR! \n", keysOrValues[i]);
            exit(0);
        }
    }
    end_time = clock();
    time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    ops = (double)insert_num / time_taken; 
    printf("%ld items are deleted ! takes %f sec, OPS %f \n", inserted ,time_taken ,ops);

    printf("The number of items stored in the level hash table: %ld\n", level->level_item_num[0]+level->level_item_num[1]);    
    level_destroy(level);

    // Free allocated memory
    for (int i = 0; i < insert_num + 1; i++) {
        free(keysOrValues[i]);
    }
    free(keysOrValues);

    return 0;
}
