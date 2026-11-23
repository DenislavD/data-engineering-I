# Important notes from Databricks Certified Associate Developer using Python (book 2)

## Transformations
- Narrow = local: operate on individual data partitions, no shuffling, quick (map, filter..)
- Wide  = global: require data redistribution across partitions and shuffling (writing to disk).
Dependencies between partitions. Defines job stage boundaries. (groupBy, join, sortByKey..)

## Catalyst Optimizer
- Works with the higher-level DataFrame API, not with RDDs?
- logical query plan
- rule-based optimization
- physical query plan
- cost-based optimization

## Adaptive Query Execution (AQE)
- Dynamically adjusts execution plans during runtime based on actual data statistics and hardware conditions

## Common problems
1. Small files
- high I/O overhead
- consolidate them into fewer larger ones
2. Data skew:
- uneven partitions
- salting keys for more even distribution
- custom partitioning logic
- sampling and filtering
- data preprocessing
3. Data spill:
- not enough memory, data goes to disk, a lot slower
- increase executor memory
- better partitioning
- caching intermediate results
- shuffle tuning (advanced)

## Join types
1. Shuffle join - 2 large datasets
2. Shuffle sort-merge - sorts first, more effective, for large-scale joins
3. Broadcast join - smaller df is broadcasted to all worker nodes
4. Broadcast hash - smaller df is broadcasted as hash table, equality-based joins


