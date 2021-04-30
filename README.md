# Coccoc hometest solution

[Assigment](https://github.com/sirhung1993/coccoc_hometest/tree/main/bi-dmp-de)


### Task 1 + 2

I use pipeline model [in Python](https://www.dabeaz.com/coroutines/Coroutines.pdf) and you can see in [source code](./task_1_2.py)

### Task 3:

#### Idea

Since the input data exceeds the internal storage capacity, we will use the external memory to store information during the sorting process.

We will use the files stored on disk as arrays in internal memory.

Files have similar characteristics to Linked List
- convenient for sequential access
- inconvenience for random access

=> merge sort + quick sort (if data is small)

If the array is larger than the allowed threshold, perform the partition and merge
Otherwise, use quicksort to do it in in-memory and then write to the file

### Implementation

- The array corresponds to the file on the disk and is referenced by the file pointer
- Uses k-way merge sort to reduce the need to create multiple intermediate arrays during merge (reduces IO costs)