
#include <assert.h>
#include <cstring>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

namespace fastset {

//////////////////////////////////////////////////////////
// configure:
// #define _LOG_FOR_DEBUG
// #define _LOG_FOR_METRICS
#define _LOG_FOR_ERROR

// #define DEBUG_VERIFY_AFTER_ENLARGE
// #define _DUMP_STAT_BEFORE_CLEAR

// #define TEST_SPINLOCK

// end of configure
//////////////////////////////////////////////////////////

#ifdef _LOG_FOR_DEBUG
#define LOG_DEBUG printf
#else
#define LOG_DEBUG(a, ...) NULL
#endif

#ifdef _LOG_FOR_ERROR
#define LOG_ERROR printf
#else
#define LOG_ERROR(a, ...) NULL
#endif

#define LOG_INFO printf

const int MAX_COUNT_PER_NODE = 4;
const int MAX_CAPACITY_BITS = 30; // 最多允许的 1G个点
const int DEF_CAPACITY_BITS = 12;
const int MIN_CAPACITY_BITS = 4; // 最小 16个hashNode

const int MAX_PARTITION_BITS = 8; // 最多 256个分区
const int DEF_PARTITION_BITS = 4;
const int DATA_CHUNK_SIZE = (1 << 20);
const float HASH_RATIO = 2.8;

template <class T> class CAutoLock {
  T *m_pLock;

public:
  CAutoLock(T *m) {
    m_pLock = m;
    m_pLock->lock();
  }
  ~CAutoLock() { m_pLock->unlock(); }
};

class SpinnedLock {
public:
  static void doLock(volatile uint32_t *p, const char *msg) {
#if defined(TEST_SPINLOCK)
    uint32_t tid = pthread_self();
#else
    uint32_t tid = 1;
#endif
    int n = 0;
    while (__sync_lock_test_and_set(p, tid) != 0) {
#ifdef TEST_SPINLOCK
      if (n == 0) {
        LOG_INFO("%u wait for lock (%s). lockobj: %p owner: %u\n", tid, msg, p,
                 *p);
      }
#endif
      if ((++n & 0x1ff) == 0) { // every 1024
        usleep(10);             // sleep 10 us
        // if (n > 1000000) {
        //   LOG_ERROR("doLock(%s) may be deadlock(%p) ???", msg, p);
        // }
      }
    }
#ifdef TEST_SPINLOCK
    LOG_INFO("%u locked (%s). lockobj: %p owner: %u\n", tid, msg, p, *p);
#endif
  }

  static void doUnlock(volatile uint32_t *p, const char *msg) {
#if defined(TEST_SPINLOCK)
    uint32_t tid = pthread_self();
    LOG_INFO("%u unlock (%s). lockobj: %p owner: %u,  %s\n", tid, msg, p, *p,
             tid == *p ? "ok" : "ERROR");
#endif
    __sync_lock_release(p);
  }

private:
  volatile uint32_t mutex{0};
  char name[32]{0};

public:
  SpinnedLock() {}
  ~SpinnedLock() {}

  void setName(int index) { sprintf(name, "rw_%d", index); };

  void lock() { SpinnedLock::doLock(&mutex, name); }

  void unlock() { SpinnedLock::doUnlock(&mutex, name); }
};

class Atomic {
public:
  template <class T> static inline void Add(volatile T *p, T v) {
    __sync_fetch_and_add(p, v);
  }

  template <class T> static inline void Add(T *p, T v) {
    __sync_fetch_and_add(p, v);
  }

  // template <class T> static void SetMax(T *p, int value) {
  //   T old = *p;
  //   while (!__sync_bool_compare_and_swap(p, old, std::max(old, value))) {
  //     old = *p;
  //   }
  // }
};

struct Slice {
  int len;
  unsigned char *buf;

  bool operator==(const Slice &other) {
    return this->len == other.len &&
           memcmp(this->buf, other.buf, this->len) == 0;
  }
};

class CalcHash {
public:
  static uint32_t get_(unsigned char *data, int len) {
    // 一亿次：1200ms (1456-258)
    uint32_t p = 16777619;
    uint32_t h = (uint32_t)2166136261L;
    for (int i = 0; i < len; i++) {
      h = (h ^ data[i]) * p;
    }
    return get(h);
  }

  static uint32_t get(unsigned char *data, int len) {
    // 一亿次：700ms (933-228)
    uint32_t h = len;
    uint32_t *src = (uint32_t *)data;
    for (int i = 0; i < len / sizeof(uint32_t); i++) {
      h ^= src[i];
      h += h << 5;
    }
    // 最后几个字节
    uint32_t t = 0;
    for (int i = (len & 0x7ffffff8); i < len; i++) {
      t = (t << 8) | data[i];
    }
    return get(h ^ t);
  }

  static uint32_t get(uint64_t v) {
    // 一亿次：250ms
    uint32_t h = (uint32_t)(v >> 32) ^ (uint32_t)v;
    return get((uint32_t)h);
  }

  static uint32_t get(int64_t v) { return get((uint64_t)v); }

  static uint32_t get(uint32_t v) {
    v += v << 13;
    v ^= v >> 7;
    v += v << 3;
    v ^= v >> 17;
    v += v << 5;
    return v & 0x7fffffff; // 返回的hash值最高位一定为0
  }

  static uint32_t get(int32_t v) { return get((uint32_t)v); }

  static uint32_t get(const Slice &slice) { return get(slice.buf, slice.len); }

  static uint32_t asNumber(const Slice &slice) { return slice.len; }
  static uint32_t asNumber(uint32_t v) { return v; }
  static uint32_t asNumber(uint64_t v) { return (uint32_t)v; }

  static uint32_t getShort(uint32_t v) {
    v += v << 13;
    v ^= v >> 7;
    return ((v >> 16) ^ v) & 0x7fff;
  }
};

class CBufferManager {

  using AutoLock = CAutoLock<std::mutex>;

  struct SizeItem {
    int size;
    std::mutex mutex;
    std::vector<unsigned char *> hot; // 刚用完的内存
    std::vector<unsigned char *> cool;
  };
  std::vector<SizeItem *> m_recyclers;
  std::vector<unsigned char *> m_chunks;
  int m_usedPos{0};
  bool m_cocurrent;

#ifdef _LOG_FOR_METRICS
  struct {
    int alloc_count;
    int max_alloc_size;
  } m_matrics{0, 0};
#endif

  std::mutex m_mutex;

public:
  CBufferManager(bool cocurrent) : m_cocurrent(cocurrent) {}

  ~CBufferManager() { clear(); }

  unsigned char *alloc(int size) {
    assert(size < (1 << 16));

    return _alloc(size);
  }

  void dealloc(unsigned char *buf, int size) { return _dealloc(buf, size); }

  void clear() {
    if (m_cocurrent) {
      AutoLock lock(&m_mutex);
      return _clear();
    } else {
      return _clear();
    }
  }

  void dump_stat(const char *msg) {
#ifdef _LOG_FOR_METRICS
    LOG_INFO(
        "%s mem: chunks=%ld, alloc_count=%d, max_size=%d, diff_size=%ld: [",
        msg, m_chunks.size(), m_matrics.alloc_count, m_matrics.max_alloc_size,
        m_recyclers.size());
#else
    LOG_INFO("%s mem: chunks=%ld, diff_size=%ld: [", msg, m_chunks.size(),
             m_recyclers.size());
#endif
    for (int i = 0; i < m_recyclers.size(); i++) {
      if (i == 0) {
        LOG_INFO("%d", m_recyclers[i]->size);
      } else {
        LOG_INFO(",%d", m_recyclers[i]->size);
      }
    }
    LOG_INFO("]\n");
  }

private:
  unsigned char *_alloc(int size) {
#ifdef _LOG_FOR_METRICS
    if (m_cocurrent)
      m_mutex.lock();
    m_matrics.alloc_count++;
    if (m_matrics.max_alloc_size < size)
      m_matrics.max_alloc_size = size;
    if (m_cocurrent)
      m_mutex.unlock();
#endif
    SizeItem *item = getRecycler(size);
    if (item->cool.size() > 0) {
      unsigned char *buf = item->cool.back();
      item->cool.pop_back();
      unlockItem(item);
      return buf;
    } else {
      unlockItem(item);
    }

    // alloc from m_chunks
    if (m_cocurrent)
      m_mutex.lock();
    if (m_chunks.size() == 0 || m_usedPos + size >= DATA_CHUNK_SIZE) {
      allocChunk();
    }
    unsigned char *buf = m_chunks.back() + m_usedPos;
    m_usedPos += size;
    if (m_cocurrent)
      m_mutex.unlock();
    return buf;
  }

  void _dealloc(unsigned char *buf, int count) {
    SizeItem *item = getRecycler(count);
    item->hot.push_back(buf);

    if (item->hot.size() > 1024) {
      int count = item->hot.size() / 2;
      for (int i = 0; i < count; i++) {
        item->cool.push_back(item->hot[i]);
      }
      item->hot.erase(item->hot.begin(), item->hot.begin() + count);
    }
    unlockItem(item);
  }

  void _clear() {
    for (auto it = m_chunks.begin(); it != m_chunks.end(); ++it) {
      delete[] * it;
    }
    m_chunks.clear();

    for (auto it = m_recyclers.begin(); it != m_recyclers.end(); ++it) {
      delete *it;
    }
    m_recyclers.clear();
    m_usedPos = 0;

#ifdef _LOG_FOR_METRICS
    memset(&m_matrics, 0, sizeof(m_matrics));
#endif
  }

  void allocChunk() {
    m_chunks.push_back(new unsigned char[DATA_CHUNK_SIZE]);
    m_usedPos = 0;
  }

  // count is power of 2
  SizeItem *getRecycler(int size) {
    AutoLock lock(&m_mutex);
    SizeItem *item = nullptr;
    for (int i = 0; i < m_recyclers.size(); i++) {
      item = m_recyclers[i];
      if (item->size == size) {
        lockItem(item);
        return item;
      }
    }
    item = new SizeItem;
    item->size = size;
    lockItem(item);
    m_recyclers.push_back(item);
    return item;
  }

  void lockItem(SizeItem *item) {
    if (m_cocurrent) {
      item->mutex.lock();
    }
  }

  void unlockItem(SizeItem *item) {
    if (m_cocurrent) {
      item->mutex.unlock();
    }
  }
};

class HashNodeBase {
protected:
  volatile uint32_t m_lock;
  uint16_t m_count;
  uint16_t m_capacity;

public:
  uint16_t getCount() const { return m_count; }

  void lock() { SpinnedLock::doLock(&m_lock, "node"); }

  void unlock() { SpinnedLock::doUnlock(&m_lock, "node"); }
};

template <class T> class FixedSizeHashNode : public HashNodeBase {
  const static int DATA_ITEM_SIZE = (sizeof(uint32_t) + sizeof(T));

  using HashNode = FixedSizeHashNode<T>;

private:
  T *m_pValues;
  struct {
    T m_values[MAX_COUNT_PER_NODE];
    uint32_t m_codes[MAX_COUNT_PER_NODE];
  };

public:
  T getValue(int index) const {
    if (index < MAX_COUNT_PER_NODE) {
      return m_values[index];
    } else {
      return m_pValues[index - MAX_COUNT_PER_NODE];
    }
  }

  uint32_t getCode(int index) const {
    if (index < MAX_COUNT_PER_NODE) {
      return m_codes[index];
    } else {
      return ((uint32_t *)(m_pValues + m_capacity))[index - MAX_COUNT_PER_NODE];
    }
  }

  int32_t find(const T &v) const {
    // lockup local-item
    int count = m_count < MAX_COUNT_PER_NODE ? m_count : MAX_COUNT_PER_NODE;
    for (int i = 0; i < count; i++) {
      if (m_values[i] == v) {
        return i;
      }
    }
    count = m_count - MAX_COUNT_PER_NODE;
    for (int i = 0; i < count; i++) {
      if (m_pValues[i] == v) {
        return i + MAX_COUNT_PER_NODE;
      }
    }
    return -1;
  }

  bool remove(const T &v, uint32_t hashCode) {
    int index = find(v);
    if (index < 0) {
      return false;
    }
    if (index < m_count - 1) {
      // 删除中间的，则需要把原先的最后一个，代替到当前位置
      put(index, getValue(m_count - 1), getCode(m_count - 1));
    }

    m_count--;
    return true;
  }

  bool safeAdd(CBufferManager *pBufMgr, const T &v, uint32_t hashCode) {
    if (this->find(v) >= 0) {
      return false;
    }
    int capacity = 0;
    if (m_capacity == 0 && m_count == MAX_COUNT_PER_NODE) {
      // 首次扩容
      capacity = MAX_COUNT_PER_NODE;
      m_pValues = (T *)pBufMgr->alloc(capacity * DATA_ITEM_SIZE);
      m_capacity = capacity;
    } else if (m_count == MAX_COUNT_PER_NODE + m_capacity) {
      // 已有扩展内存块，扩容
      capacity = m_capacity * 2;
      T *pValues = (T *)pBufMgr->alloc(capacity * DATA_ITEM_SIZE);
      memcpy(pValues, m_pValues, sizeof(T) * (m_capacity));
      memcpy(pValues + capacity, m_pValues + m_capacity,
             sizeof(uint32_t) * m_capacity);

      unsigned char *pOldBuf = (unsigned char *)m_pValues;
      int bufLen = m_capacity * DATA_ITEM_SIZE;

      m_pValues = pValues;
      m_capacity = capacity;

      // delay dealloc here, when node is ready (for thread-safe)
      pBufMgr->dealloc(pOldBuf, bufLen);
    }
    put(m_count, v, hashCode);
    m_count++;
    return true;
  }

  int split(CBufferManager *pBufMgr, HashNode *other, int capacity) {
    // 在rehash时，运行并行加入的同样数据，加入到新节点或老节点，因此这里返回
    // 迁移节点时的重复个数
    int newCount = 0;
    int dupCount = 0;
    for (int index = 0; index < m_count; index++) {
      uint32_t hashCode = getCode(index);
      T v = getValue(index);
      if (hashCode & capacity) {
        if (!other->safeAdd(pBufMgr, v, hashCode)) {
          dupCount++;
        }
      } else {
        if (index != newCount) {
          // move forward
          put(newCount, v, hashCode);
        }
        newCount++;
      }
    }
    m_count = newCount;
    return dupCount;
  }

  void dump(const char *msg) const {
    LOG_INFO("%s: Node_%p:(lock=%u,cap=%d,cnt=%d):", msg, this, m_lock,
             m_capacity, m_count);
    for (int i = 0; i < m_count; i++) {
      LOG_INFO(" %x", getCode(i));
    }
    LOG_INFO("\n");
  }

  bool debug_verify(int hashIndex, int hashMask) const {
    for (int i = 0; i < m_count; i++) {
      if (!debug_verify(hashIndex, hashMask, i)) {
        return false;
      }
    }
    return true;
  }

  bool debug_verify(int hashIndex, int hashMask, int item) const {
    uint32_t hashCode = getCode(item);
    int expectedIndex = hashCode & hashMask;
    if (expectedIndex != hashIndex) {
      char buf[256] = {0};
      sprintf(buf, "inconsist hash value: %x: (%x & %x) != %x, diff=%x",
              expectedIndex, hashCode, hashMask, hashIndex,
              expectedIndex ^ hashIndex);
      dump(buf);
      return false;
    }
    return true;
  }

private:
  void put(int index, const T &v, uint32_t hashCode) {
    // call put before m_count is increased
    // assert(index < m_count);
    if (index < MAX_COUNT_PER_NODE) {
      m_values[index] = v;
      m_codes[index] = hashCode;
    } else {
      m_pValues[index - MAX_COUNT_PER_NODE] = v;
      ((uint32_t *)(m_pValues + m_capacity))[index - MAX_COUNT_PER_NODE] =
          hashCode;
    }
  }
};

class SliceHashNode : public HashNodeBase {

  using HashNode = SliceHashNode;

  struct ItemInfo {
    uint32_t code;
    uint16_t len; //
    uint16_t off; // 从尾部计算的距离，（使得可以整块负责数据）
  };

private:
  unsigned char *m_pBuffer; // Info1, 2, ..., (..free..) (m_offset)..., Data1
  uint32_t m_usedSpace;     // 以及占用的空间

public:
  Slice getValue(int index) const {
    ItemInfo *pItem = getItem(index);
    return Slice{pItem->len, m_pBuffer + m_capacity - pItem->off};
  }

  uint32_t getCode(int index) const {
    const ItemInfo *pItem = getItem(index);
    return pItem->code;
  }

  int32_t find(const Slice &v) const {
    for (int i = 0; i < m_count; i++) {
      ItemInfo *pItem = getItem(i);
      if (v.len == pItem->len &&
          memcmp(m_pBuffer + m_capacity - pItem->off, v.buf, pItem->len) == 0)
        return i;
    }
    return -1;
  }

  bool remove(const Slice &v, uint32_t hashCode) {
    int index = find(v);
    if (index < 0) {
      return false;
    }
    if (index < m_count - 1) {
      // 删除中间的，把最后一个 itemInfo填写到当前位置
      // 其余内存不做修改和移动（也不会重新利用原先的内存）
      ItemInfo *p = getItem(index);
      *p = *getItem(m_count - 1);
    }

    m_count--;
    return true;
  }

  bool safeAdd(CBufferManager *pBufMgr, const Slice &v, uint32_t hashCode) {
    if (this->find(v) >= 0) {
      return false;
    }
    int needSpace = sizeof(ItemInfo) + getAlignedSize(v.len) + getUsedSpace();
    if (m_capacity == 0) {
      int capacity = calcNeedCapacity(needSpace);
      m_pBuffer = pBufMgr->alloc(capacity);
      m_capacity = capacity;
    } else if (m_capacity < needSpace) {
      // 已有扩展内存块，需要扩容
      int capacity = calcNeedCapacity(needSpace);
      unsigned char *pBuffer = pBufMgr->alloc(capacity);
      // 复制现有数据。（记录的offset为从尾部开始计算，因此相对位置不变，无需逐项更新）
      memcpy(pBuffer + capacity - m_usedSpace,
             m_pBuffer + m_capacity - m_usedSpace, m_usedSpace);
      // 复制DataItem信息。（尾部对齐）
      memcpy(pBuffer, m_pBuffer, m_count * sizeof(ItemInfo));

      unsigned char *pOldBuf = m_pBuffer;
      int bufLen = m_capacity;

      m_pBuffer = pBuffer;
      m_capacity = capacity;

      // delay dealloc here, when node is ready (for thread-safe)
      pBufMgr->dealloc(pOldBuf, bufLen);
    }
    put(m_count, v, hashCode, m_usedSpace);
    m_count++;
    return true;
  }

  int split(CBufferManager *pBufMgr, HashNode *other, int capacity) {
    // 在rehash时，运行并行加入的同样数据，加入到新节点或老节点，因此这里返回
    // 迁移节点时的重复个数
    int newCount = 0;
    int dupCount = 0;
    uint32_t usedSpace = 0;
    Slice v;
    for (int index = 0; index < m_count; index++) {
      ItemInfo *pInfo = getItem(index);
      v.len = pInfo->len;
      v.buf = getValuePtr(pInfo);
      if (pInfo->code & capacity) {
        if (!other->safeAdd(pBufMgr, v, pInfo->code)) {
          dupCount++;
        }
      } else {
        if (index != newCount) {
          // move forward
          put(newCount, v, pInfo->code, usedSpace);
        } else {
          // 无需复制数据
          usedSpace += getAlignedSize(v.len);
        }

        newCount++;
      }
    }
    m_count = newCount;
    m_usedSpace = usedSpace;
    return dupCount;
  }

  void dump(const char *msg) const {
    LOG_INFO("%s: Node_%p:(lock=%u,cap=%d,cnt=%d):", msg, this, m_lock,
             m_capacity, m_count);
    for (int i = 0; i < m_count; i++) {
      LOG_INFO(" %x", getCode(i));
    }
    LOG_INFO("\n");
  }

  bool debug_verify(int hashIndex, int hashMask) const {
    for (int i = 0; i < m_count; i++) {
      if (!debug_verify(hashIndex, hashMask, i)) {
        return false;
      }
    }
    return true;
  }

  bool debug_verify(int hashIndex, int hashMask, int item) const {
    uint32_t hashCode = getCode(item);
    int expectedIndex = hashCode & hashMask;
    if (expectedIndex != hashIndex) {
      char buf[256] = {0};
      sprintf(buf, "inconsist hash value: %x: (%x & %x) != %x, diff=%x",
              expectedIndex, hashCode, hashMask, hashIndex,
              expectedIndex ^ hashIndex);
      dump(buf);
      return false;
    }
    return true;
  }

private:
  int32_t getAlignedSize(int size) const { return (size + 7) & 0x7ffffff8; }

  int32_t calcNeedCapacity(int need) const {
    int capacity = m_capacity ? m_capacity : 64;
    while (capacity < need) {
      capacity = capacity << 1;
    }
    return capacity;
  }

  void put(int index, const Slice &v, uint32_t hashCode, uint32_t &usedSpace) {
    // call put before m_count is increased
    ItemInfo *pInfo = getItem(index);
    usedSpace += getAlignedSize(v.len);
    pInfo->code = hashCode;
    pInfo->len = v.len;
    pInfo->off = usedSpace;
    // split时，内存可能出现overlap
    memmove(getValuePtr(pInfo), v.buf, v.len);
  }

  ItemInfo *getItem(int index) const { return (ItemInfo *)m_pBuffer + index; }

  unsigned char *getValuePtr(ItemInfo *pItem) const {
    return m_pBuffer + m_capacity - pItem->off;
  }

  int32_t getUsedSpace() const {
    return sizeof(ItemInfo) * m_count + m_usedSpace;
  }
};

template <class T, class HashNode> class PartitionImpl {

  using Partition = PartitionImpl<T, HashNode>;
  using NodeAutoLock = CAutoLock<HashNode>;

private:
  // 多个线程访问，需要同步，避免编译器优化
  // 扩容完成时，对hashMask及rehashedIndex的修改需要一次完成
  union EnlargeStatus {
    uint64_t value;
    struct {
      int hashMask;
      int rehashedIndex;
    };
  };
  volatile EnlargeStatus m_status;
  volatile int m_enlarging{0};
  volatile int m_count{0};

  int m_tableSize{0};
  int m_usedTableEntries{0};
  HashNode **m_table{nullptr};
  CBufferManager *m_bufMgr{nullptr};
  bool m_cocurrent{false};
  int m_nextEnlargingSize{0};
  int m_partIndex{0};

  int m_initCapacityBits{0};
  int m_nodeCountPerChunk{0};

  std::mutex m_rwmutex;

#ifdef DEBUG_VERIFY_AFTER_ENLARGE
  volatile bool m_blocking{false};
#endif

public:
  PartitionImpl(bool cocurrent, int partIndex, int initCapacityBits)
      : m_cocurrent(cocurrent), m_partIndex(partIndex),
        m_initCapacityBits(initCapacityBits) {

    m_nodeCountPerChunk = 1 << initCapacityBits;

    m_tableSize = (1 << (MAX_CAPACITY_BITS - initCapacityBits + 1));
    m_table = new HashNode *[m_tableSize];
    m_bufMgr = new CBufferManager(cocurrent);

    allocNodeChunk(1);
    m_status.hashMask = (1 << m_initCapacityBits) - 1;
    m_status.rehashedIndex = -1;
    m_nextEnlargingSize = HASH_RATIO * (m_status.hashMask + 1);
  }

  ~PartitionImpl() {
    _clear(false);
    delete[] m_table;
    delete m_bufMgr;
  }

  void clear() { _clear(true); }

  HashNode *getNode(int index) const {
    // assert((index >> m_initCapacityBits) < this->m_usedTableEntries);
    return m_table[index >> m_initCapacityBits] +
           (index & (m_nodeCountPerChunk - 1));
  }

  int getMask() const { return m_status.hashMask; }

  int size() const { return m_count; }

  bool add(const T &v, uint32_t hashCode) {
    int hashIndex = hashCode & m_status.hashMask;
    if (!m_cocurrent) {
      HashNode *node = this->getNode(hashIndex);
      if (node->safeAdd(m_bufMgr, v, hashCode)) {
        m_count++;
        tryEnlargeHashTable();
        return true;
      }
      return false;
    }

    return cocurrentAdd(v, hashCode);
  }

#ifdef DEBUG_VERIFY_AFTER_ENLARGE
  bool waitForUnblock() {
    while (m_blocking) {
      usleep(100);
    }
    return true;
  }
#endif

  bool cocurrentAdd(const T &v, uint32_t hashCode) {
    // 利用 rehashedIndex 的值，来避免上锁（m_rwmutex）
    // 未扩区时，rehashedIndex = -1。（此时高区不可用，或无任何顶点已经分裂）
    // 扩区过程中 rehashedIndex 为实际完成分裂的节点编号。（高区可用）
    // 扩区结束时，hashMask 与 rehashedIndex 同时更新
    // 取到node锁后，rehashedIndex 不会出现增加超过当前节点的情况
    //    可以保证：当前需要操作的node要不然完成分裂，要不尚未开始分裂

#ifdef DEBUG_VERIFY_AFTER_ENLARGE
    waitForUnblock();
#endif

    uint32_t hashMask = m_status.hashMask;
    int hashIndex = hashCode & hashMask;
    HashNode *node = this->getNode(hashIndex);
    node->lock();
    // 在加锁过程中，可能已经完成多轮扩区
    while (hashMask != m_status.hashMask) {
      node->unlock();
      hashMask = m_status.hashMask;
      hashIndex = hashCode & hashMask;
      node = this->getNode(hashIndex);
      node->lock();
    }

    if (hashCode & (hashMask + 1)) {
      // 扩区中，新的值属于分裂后的新节点，可能需加入到分裂后的新节点
      EnlargeStatus s;
      s.value = m_status.value;
      if ((hashIndex <= s.rehashedIndex) ||
          (s.rehashedIndex == -1 && hashMask != s.hashMask)) {
        // 第一个条件：扩区进行中，且当前节点已经分裂。(不管mask是否已经更新)
        // 第二个条件：自从获取 hashMask 后，扩区刚完成
        hashIndex = hashIndex + hashMask + 1;
        HashNode *node2 = this->getNode(hashIndex);
        node2->lock();
        node->unlock();
        node = node2;
      }
    }
    bool ret = node->safeAdd(m_bufMgr, v, hashCode);
    if (ret) {
      // 放在这里，m_count 才正确？？？！！！
      Atomic::Add(&m_count, 1);
    }
    node->unlock();

    if (ret) {
      // 放在这里，不正确？？
      // Atomic::Add(&m_count, 1);
      tryEnlargeHashTable();
    }

    return ret;
  }

  int find(const T &v, uint32_t hashCode, int &hashIndex) const {
    int hashMask = m_status.hashMask;
    hashIndex = hashCode & hashMask;
    int32_t itemIndex = getNode(hashIndex)->find(v);
    if (itemIndex < 0) {
      // 目标分区可能正在扩区。（内存已经ready）。
      // 由于node不加锁，只要高区可用就需要搜索高区
      if (m_cocurrent && m_enlarging == 2 && (hashCode & (hashMask + 1))) {
        // 存在扩表，且当前节点可能存在移动
        // 检查新节点，不用锁定
        hashIndex = hashIndex + hashMask + 1;
        return getNode(hashIndex)->find(v);
      }
    }
    return itemIndex;
  }

  int addAll(Partition *pSrc) {
    // 本函数不支持并发，this和 pSrc均不存在其他线程的修改
    // 把src的全部内容加入到当前分区中。返回成功加入的个数
    // 两者的 hashMask可能不同，因此需要逐个处理
    int n = 0;
    for (int srcIndex = 0; srcIndex <= pSrc->m_status.hashMask; srcIndex++) {
      HashNode *srcNode = pSrc->getNode(srcIndex);
      for (int i = 0; i < srcNode->getCount(); i++) {
        if (this->add(srcNode->getValue(i), srcNode->getCode(i))) {
          n++;
        }
      }
    }
    return n;
  }

  bool remove(const T &v, uint32_t hashCode) {
    // 现有外部调用时，remove为单线程，且与add不会并发
    // 为简化，这里只考虑与 add/remove 的并发操作，不考虑因add产生扩容时并发问题
    // 最不好的结果是：在扩容时，可能不能正确删除
    int hashIndex = hashCode & m_status.hashMask;
    HashNode *node = this->getNode(hashIndex);
    if (m_cocurrent) {
      node->lock();
    }
    bool ret = node->remove(v, hashCode);
    if (m_cocurrent) {
      node->unlock();
    }

    if (ret) {
      Atomic::Add(&m_count, -1);
    }
    return ret;
  }

  int debug_verify(int partIndex) const {
    int nErrCount = 10;
    int count = m_usedTableEntries * m_nodeCountPerChunk;
    int total = 0;
    int mid = (this->m_status.hashMask + 1) >> 1;
    for (int i = 0; i < count; i++) {
      auto node = getNode(i);
      total += node->getCount();
      if (!node->debug_verify(i, this->m_status.hashMask)) {
        if (i < mid) {
          auto node2 = getNode(i + mid);
          char buf[256];
          sprintf(buf, "high-part(%x)", i + mid);
          node2->dump(buf);
        }
        if (--nErrCount == 0) {
          break;
        }
      }
    }
    return total;
  }

  void dump_stat(const char *msg) const {
    struct {
      int hist[4];   // for count is zero and one
      int max_len;   // max len of node
      int max_index; // node index with max len
      long total;    // total len of node
      long square;
    } m{0};

    int count = m_usedTableEntries * m_nodeCountPerChunk;
    for (int i = 0; i < count; i++) {
      auto node = getNode(i);
      int c = node->getCount();
      m.total += c;
      m.square += c * c;

      if (c > m.max_len) {
        m.max_len = c;
        m.max_index = i;
      }
      if (c < 4)
        m.hist[c]++;
    }

    float avg = (float)m.total / count;
    float std = sqrt((float)m.square / count - avg * avg);
    LOG_INFO("%s nodes=%d max=%d(%d) avg=%.3f std=%.3f hist=[%d", msg, count,
             m.max_len, m.max_index, avg, std, m.hist[0]);
    for (int i = 1; i < 4; i++) {
      LOG_INFO(",%d", m.hist[i]);
    }

    this->m_bufMgr->dump_stat("]");
  }

private:
  void _clear(bool withInit) {
    m_bufMgr->clear();
    int toKeep = 0;

    if (withInit) {
      if (this->m_usedTableEntries > 0) {
        // 重用1块内存，降低内存申请释放开销，及缺页中断机会
        toKeep = 1;
        memset(m_table[0], 0, sizeof(HashNode) * m_nodeCountPerChunk);
      }
      m_status.hashMask = (1 << m_initCapacityBits) - 1;
      m_nextEnlargingSize = HASH_RATIO * (m_status.hashMask + 1);
    }

    for (int i = toKeep; i < this->m_usedTableEntries; i++) {
      delete[] m_table[i];
    }
    m_usedTableEntries = toKeep;
    m_count = 0;
  }

  void allocNodeChunk(int count = 1) {
    // 除第一个外，之后每个都必须翻倍
    assert(m_usedTableEntries == 0 || m_usedTableEntries == count);
    assert(m_usedTableEntries + count < this->m_tableSize);
    for (int i = 0; i < count; i++) {
      HashNode *nodes = new HashNode[m_nodeCountPerChunk];
      memset(nodes, 0, sizeof(HashNode) * m_nodeCountPerChunk);
      m_table[m_usedTableEntries] = nodes;
      m_usedTableEntries++;
    }
  }

  bool needEnlargeHashTable() const {
    if (m_enlarging || m_count <= m_nextEnlargingSize)
      return false;

    return true;
  }

  void tryEnlargeHashTable() {
    // 检查扩容需要加锁，先检查一次
    if (!needEnlargeHashTable())
      return;

    if (m_cocurrent) {
      m_rwmutex.lock();
    }

    // 加锁后，重复检查
    if (needEnlargeHashTable()) {
      // 拒绝其他线程进入
      m_enlarging = 1;
      assert(m_status.rehashedIndex == -1);
      int capacity = m_status.hashMask + 1;

      if (m_cocurrent) {
        m_rwmutex.unlock();
      }

      // 耗时操作开始（已经解锁）
      this->enlargeHashTable(capacity);

      if (m_cocurrent) {
        m_rwmutex.lock();
      }

      m_enlarging = 0;

      EnlargeStatus s;
      s.rehashedIndex = -1;
      s.hashMask = capacity + m_status.hashMask;
      // 使用原子操作
      m_status.value = s.value;

      if (m_status.hashMask < ((1 << MAX_CAPACITY_BITS) - 1))
        m_nextEnlargingSize = HASH_RATIO * (m_status.hashMask + 1);

      if (m_cocurrent) {
        m_rwmutex.unlock();
      }

#ifdef DEBUG_VERIFY_AFTER_ENLARGE
      m_blocking = true;
      this->debug_verify(m_partIndex);
      m_blocking = false;
#endif

    } else if (m_cocurrent) {
      m_rwmutex.unlock();
    }
  }

  void enlargeHashTable(int capacity) {
    // 扩展hashTable，按当前容量翻倍
    allocNodeChunk(capacity / m_nodeCountPerChunk);

    // 进入第二阶段，高区表已经可用。
    this->m_enlarging = 2;

    int dupCount = 0;
    for (int i = 0; i < capacity; i++) {
      HashNode *node1 = getNode(i);
      HashNode *node2 = getNode(capacity + i);

      node1->lock();
      node2->lock();

      dupCount += node1->split(this->m_bufMgr, node2, capacity);
      m_status.rehashedIndex = i;

      node2->unlock();
      node1->unlock();
    }
  }

  void dump(const char *msg) const {
    LOG_INFO("dump %s, capacity=%d, count=%d\n", msg, m_status.hashMask + 1,
             m_count);
    int count = 0;
    char buf[256] = {0};
    for (int i = 0; i <= m_status.hashMask; i++) {
      sprintf(buf, "[%d]: ", i);
      HashNode *node = getNode(i);
      node->dump(buf);
      count += node->getCount();
    }
    LOG_INFO("dump finish: %d (expected: %d)\n", count, m_count);
    assert(count == m_count);
  }
};

template <class T, class HashNode> class FastHashSetImpl {

  using FastHashSet = FastHashSetImpl<T, HashNode>;
  using Partition = PartitionImpl<T, HashNode>;

public:
  class iterator {
    const FastHashSet *m_owner;
    int m_partIndex;
    int m_hashIndex;
    int m_itemIndex;

  public:
    iterator(const FastHashSet *owner, int partIndex, int hashIndex,
             int itemIndex)
        : m_owner(owner), m_partIndex(partIndex), m_hashIndex(hashIndex),
          m_itemIndex(itemIndex) {}

    iterator(const iterator &it)
        : m_owner(it.m_owner), m_partIndex(it.m_partIndex),
          m_hashIndex(it.m_hashIndex), m_itemIndex(it.m_itemIndex) {}

    iterator &operator++() { // ++it
      next();
      return *this;
    }

    iterator operator++(int) { // it++
      iterator prev(*this);
      next();
      return prev;
    }

    T operator*() const {
      if (m_partIndex >= 0) {
        HashNode *node = m_owner->getNode(m_partIndex, m_hashIndex);
        return node->getValue(m_itemIndex);
      }
      return T{0};
    }

    // iterator &operator=() { return *this; }

    bool operator==(const iterator &other) const {
      return this->m_owner == other.m_owner &&
             this->m_partIndex == other.m_partIndex &&
             this->m_hashIndex == other.m_hashIndex &&
             this->m_itemIndex == other.m_itemIndex;
    }

    bool operator!=(const iterator &other) const {
      return this->m_owner != other.m_owner ||
             this->m_partIndex != other.m_partIndex ||
             this->m_hashIndex != other.m_hashIndex ||
             this->m_itemIndex != other.m_itemIndex;
    }

    bool hasNext() const {
      int partIndex = m_partIndex;
      int itemIndex = m_itemIndex + 1;
      int hashIndex = m_hashIndex;
      while (partIndex >= 0 && partIndex < m_owner->getPartitionCount()) {
        Partition *p = m_owner->getPartition(partIndex);
        while (hashIndex <= p->getMask()) {
          HashNode *node = p->getNode(hashIndex);
          if (itemIndex < node->getCount()) {
            return true;
          }
          itemIndex = 0;
          hashIndex++;
        }
        hashIndex = 0;
        partIndex++;
      }
      return false;
    }

    bool isValid() const {
      if (m_partIndex >= 0 && m_partIndex < m_owner->getPartitionCount()) {
        HashNode *node = m_owner->getNode(m_partIndex, m_hashIndex);
        if (m_itemIndex < node->getCount()) {
          return true;
        }
      }
      return false;
    }

    void dump() const {
      HashNode *node = m_owner->getNode(m_partIndex, m_hashIndex);
      char buf[256];
      sprintf(buf, "iterate(%d,%d,%d)=(%d)", m_partIndex, m_hashIndex,
              m_itemIndex, node->getCode(m_itemIndex));
      node->dump(buf);
    }

    void debug_verify() const {
      Partition *p = m_owner->getPartition(m_partIndex);
      HashNode *node = p->getNode(m_hashIndex);
      node->debug_verify(m_hashIndex, p->getMask(), m_itemIndex);
    }

  private:
    void next() {
      if (m_partIndex == -1 || m_partIndex == m_owner->getPartitionCount()) {
        // at the end ...
        return;
      }
      while (m_partIndex < m_owner->getPartitionCount()) {
        Partition *p = m_owner->getPartition(m_partIndex);
        HashNode *node = p->getNode(m_hashIndex);
        if (++m_itemIndex < node->getCount()) {
          return;
        }
        m_itemIndex = 0;
        while (++m_hashIndex <= p->getMask()) {
          node = p->getNode(m_hashIndex);
          if (m_itemIndex < node->getCount()) {
            return;
          }
        }
        m_hashIndex = 0;
        m_itemIndex = -1;
        m_partIndex++;
      }
      m_partIndex = -1;
      m_hashIndex = 0;
      m_itemIndex = 0;
      return;
    }
  };

private:
  bool m_cocurrent{false};
  int m_partitionCount{0};
  Partition **m_partitions;
  iterator _end{this, -1, 0, 0};

public:
  FastHashSetImpl(bool cocurrent, int partitionsBits, int initCapacityBits)
      : m_cocurrent(cocurrent) {
    if (partitionsBits < 0 ) {
      partitionsBits = DEF_PARTITION_BITS;
    } else if(partitionsBits > MAX_PARTITION_BITS) {
      partitionsBits = MAX_PARTITION_BITS;
    }
    m_partitionCount = 1 << partitionsBits;
    m_partitions = new Partition *[m_partitionCount];

    if (initCapacityBits < MIN_CAPACITY_BITS ||
        initCapacityBits > MAX_CAPACITY_BITS) {
      initCapacityBits = DEF_CAPACITY_BITS;
    }

    for (int i = 0; i < m_partitionCount; i++) {
      m_partitions[i] = new Partition(m_cocurrent, i, initCapacityBits);
    }
  }

  ~FastHashSetImpl() {
    for (int i = 0; i < m_partitionCount; i++) {
      delete m_partitions[i];
    }
    delete[] m_partitions;
  }

  inline int getPartitionCount() const { return m_partitionCount; }

  inline int getPartitionIndex(uint32_t hashCode) const {
    return CalcHash::getShort(hashCode) & (m_partitionCount - 1);
  }

  inline Partition *getPartitionByHashCode(uint32_t hashCode) const {
    int partIndex = getPartitionIndex(hashCode);
    return m_partitions[partIndex];
  }

  inline Partition *getPartition(int partIndex) const {
    return m_partitions[partIndex];
  }

  bool add(const T &v) {
    uint32_t hashCode = CalcHash::get(v);
    Partition *p = getPartitionByHashCode(hashCode);
    return p->add(v, hashCode);
  }

  int addAll(iterator begin, iterator end) {
    int n = 0;
    for (iterator it = begin; it != end; ++it) {
      if (this->add(*it)) {
        n++;
      }
    }
    return n;
  }

  int addAll(FastHashSet *other) {
    if (this->m_partitionCount == other->getPartitionCount()) {
      // 两者的分区一致，直接对拷
      int n = 0;
      for (int i = 0; i < m_partitionCount; i++) {
        Partition *src = other->getPartition(i);
        n += getPartition(i)->addAll(src);
      }
      return n;
    }
    // 使用重新插入的方式
    return addAll(other->begin(), other->end());
  }

  bool addExclusive(const T &v, const FastHashSet *other) {
    // 如果 v 在 other 中不存在，则加入到this中。否则不加入
    // 只需要计算一次hash
    uint32_t hashCode = CalcHash::get(v);
    if (other != nullptr) {
      int hashIndex = 0;
      Partition *p = other->getPartitionByHashCode(hashCode);
      if (p->find(v, hashCode, hashIndex) >= 0) {
        return false;
      }
    }
    Partition *p = getPartitionByHashCode(hashCode);
    return p->add(v, hashCode);
  }

  bool contains(const T &v) const {
    uint32_t hashCode = CalcHash::get(v);
    iterator it = _find(v, hashCode);
    return it != _end;
  }

  bool remove(const T &v) {
    uint32_t hashCode = CalcHash::get(v);
    Partition *p = getPartitionByHashCode(hashCode);
    return p->remove(v, hashCode);
  }

  int erase(iterator it, int count) {
    // 单线程
    int n = 0;
    for (int i = 0; i < count && it != end(); i++) {
      if (this->remove(*it)) {
        n++;
        // 删除时，同一node下的后续数据将代替当前迭代器位置（当前迭代器仍有效）
        // 如果删除了node的最后一项，则当前迭代器无效，需要往下跑一个（可能跑到
        // end）
        if (!it.isValid()) {
          ++it;
        }
      }
    }
    return n;
  }

  size_t size() const {
    int count = 0;
    for (int i = 0; i < m_partitionCount; i++) {
      count += getPartition(i)->size();
    }
    return count;
  }

  void clear() {
#ifdef _DUMP_STAT_BEFORE_CLEAR
    dump_stat();
#endif
    for (int i = 0; i < m_partitionCount; i++) {
      getPartition(i)->clear();
    }
  }

  void debug_verify() const {
    int count = 0;
    int total = 0;
    int sum_count = 0;
    int sum_total = 0;
    for (int i = 0; i < m_partitionCount; i++) {
      count = getPartition(i)->size();
      total = getPartition(i)->debug_verify(i);
      if (count != total) {
        char buf[256]{0};
        sprintf(buf, "partition[%d]: incorrect size: %d, actual: %d ", i, count,
                total);
        getPartition(i)->dump_stat(buf);
      }
      sum_count += count;
      sum_total += total;
    }
    if (sum_total != sum_count) {
      LOG_INFO("incorrect size: %d, actual: %d\n", sum_count, sum_total);
    }
  }

  void dump_stat() const {
    char buf[128];
    for (int i = 0; i < m_partitionCount; i++) {
      sprintf(buf, "partition_%d ", i);
      getPartition(i)->dump_stat(buf);
    }
  }

public: // for iterator
  iterator begin() const {
    iterator it{this, 0, 0, -1};
    return ++it;
  }

  const iterator &end() const { return this->_end; }

  iterator find(const T &v) const {
    uint32_t hashCode = CalcHash::get(v);
    return _find(v, hashCode);
  }

  HashNode *getNode(int partIndex, int hashIndex) const {
    return getPartition(partIndex)->getNode(hashIndex);
  }

private:
  iterator _find(const T &v, uint32_t hashCode) const {
    int partIndex = getPartitionIndex(hashCode);
    int hashIndex = 0;
    int itemIndex = getPartition(partIndex)->find(v, hashCode, hashIndex);
    if (itemIndex >= 0) {
      return iterator(this, partIndex, hashIndex, itemIndex);
    }

    return _end;
  }
};

template <class T>
class CSimpleHashSet : public FastHashSetImpl<T, FixedSizeHashNode<T>> {
  using FastHashSet = FastHashSetImpl<T, FixedSizeHashNode<T>>;

public:
  CSimpleHashSet(bool cocurrent, int partitionBits = DEF_PARTITION_BITS,
                 int capacityBits = DEF_CAPACITY_BITS)
      : FastHashSet(cocurrent, partitionBits, capacityBits) {}
};

class CSliceHashSet : public FastHashSetImpl<Slice, SliceHashNode> {
  using FastHashSet = FastHashSetImpl<Slice, SliceHashNode>;

public:
  CSliceHashSet(bool cocurrent, int partitionBits = DEF_PARTITION_BITS,
                int capacityBits = DEF_CAPACITY_BITS)
      : FastHashSet(cocurrent, partitionBits, capacityBits) {}
};

} // namespace fastset
