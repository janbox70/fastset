# fastset

(2022/8/15, by xujb)

## 1. 说明
fastset 是一个高性能的hashset，主要用于巨大数据量下的只增数据的集合（消重）。

## 2. 主要特性及用法
### 2.1 特性
- 支持数据的add，find，addAll，contains，remove等操作，支持begin/end及迭代器
- 支持固定长度变量的CSimpleHashSet，也支持不定长度的CSliceHashSet
- 是c++实现的，也提供jni接口，作为java的堆外内存，降低java进程的GC开销
- 具备极高性能，64位整数 插入性能是std::unordered_set的 3倍以上，查询性能2倍，迭代性能 5倍，清理/析构性能50倍
- 采用自身内存管理机制，不会产生大量的小内存碎片
- 支持线程安全及不安全的版本，线程安全版本使用CAS等进行无锁化处理，性能接近与单线程版本

### 2.2 用法说明
- fastset 核心有两个类，基本用法同std::unordered_set
    - CSimpleHashSet<T>，其中T为固定大小的原始数据类型，如int64
    - CSliceHashSet，为可变长的类型的HashSet，数据类型为 Slice，其中包含一个长度及内存指针。变长类型的单个数据长度最多不超过 32 KB
- 主要方法
    - 构造函数参数：
        - concurrent，表示是否支持线程安全。false为线程不安全，但性能更好
        - partitionBits，表示分区数的位数（用于多线程下，降到碰撞几率），默认值为 4，表示 (1<<4) 即16个分区
        - capacityBits，表示单个分区初始节点数的位数，默认值为12，表示 (1<<12) 即4096个hash节点。过小的值会导致扩容次数增加而影响性能。
    - add(v): 增加一个数据项，add时，SliceHashset会复制数据，因此在add结束后，调用者可以自行处理指针及相关内存
    - addAll(other)：把另一个fastset的内容加入到当前的fastset
    - addExclusive(v, other)：加入数据项时，仅当该数据项在另外一个fastset中不存在时才加入
    - remove: 删除数据项（出于性能考虑，多线程下与add同时操作时，可能不能删除数据）
    - erase(iterator, count): 从指定的迭代器位置开始删除count个数据项，返回实际删除数量
    - contains：检查 set 中是否包含指定数据
    - find: 获取指定数据的迭代器
    - clear: 清空数据
- 迭代器
    - 通过 begin/end获取 iterator，iterator 可以递增（++），取值(*)，比较（==）
    - hashset对象析构后，迭代器不能继续使用
    - 迭代顺序不保证顺序（与插入顺序及hash值有关）

## 3. 源码说明及编译

```
src/fasthashset.h：为固定长度的fastset的实现
src/test_hashset.cpp：为测试程序，提供性能测试及单元功能正确性测试
jni/*：  为 jni接口
```

### 3.1 编译测试程序
在linux下使用make命令：
```
make test
```
或在windows下使用：
```
mingw32-make -f Makefile_win32
```

### 3.2 编译jni
编译需要配置正确的JAVA_HOME环境变量
在linux下使用make命令：
```
make jniset
```
或在windows下使用：
```
 mingw32-make -f Makefile_win32 jniset
```


## 4. 性能测试
### 4.1 测试说明
- 测试环境：在一台 64 核的服务器上进行
- 测试数据：twitter2010，前一亿条边的目标顶点，消除后为 17038567
- 总体性能：64位整数 插入性能是std::unordered_set的 3倍以上，查询性能2倍，迭代性能 5倍，清理/析构性能50倍

### 4.2 单线程下的性能（test_hashset）
```
==== test LongHashSet...
HashSet add 100000000, 17038567, cost: 9570
HashSet contains 100000000, 100000000, cost: 7481
HashSet iterate 17038567, 625140560737493, cost: 215
HashSet addAll 17038567, cost: 627
HashSet clear cost: 30
HashSet addAll by iterator 17038567, cost: 710
HashSet clear2 cost: 25
==== test LongCocurrentHashset...
CocurrentHashset add 100000000, 17038567, cost: 11874
CocurrentHashset contains 100000000, 100000000, cost: 7412
CocurrentHashset iterate 17038567, 625140560737493, cost: 214
CocurrentHashset addAll 17038567, cost: 861
CocurrentHashset clear cost: 28
CocurrentHashset addAll by iterator 17038567, cost: 955
CocurrentHashset clear2 cost: 25
==== test unordered_set...
std::unordered_set add 100000000, 17038567, cost: 33185
std::unordered_set find 100000000, 100000000, cost: 13152
std::unordered_set iterate 17038567, 625140560737493, cost: 1174
std::unordered_set addAll 17038567, cost: 4765
std::unordered_set clear cost: 1253
```

### 4.3 多线程性能（test_hashset）


## 5. jni性能测试

### 64线程，twitter数据集前 1亿 行（其中包含消重后的数量为 17038567
```
Test 5 Java HashMapThreads is 64
Add time is 740 contains time is 858 iterator time is 433 set size is 17038567
Add time is 877 contains time is 943 iterator time is 418 set size is 17038567
Add time is 1265 contains time is 1735 iterator time is 434 set size is 17038567
Add time is 1692 contains time is 2187 iterator time is 426 set size is 17038567
Add time is 1882 contains time is 2397 iterator time is 429 set size is 17038567
5 total time is 18748

``` 

启用 jemalloc：
```
export LD_PRELOAD=/usr/local/lib/libjemalloc.so
```

``` 
Test 5  FastSet
Add time is 943 contains time is 836 iterator time is 428 set size is 17038567
Add time is 772 contains time is 550 iterator time is 437 set size is 17038567
Add time is 1148 contains time is 510 iterator time is 424 set size is 17038567
Add time is 748 contains time is 557 iterator time is 420 set size is 17038567
Add time is 746 contains time is 483 iterator time is 424 set size is 17038567
5 total time is 11230
```
