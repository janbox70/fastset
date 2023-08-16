
#include "fasthashset.h"
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>

using LongHashset = fastset::CSimpleHashSet<uint64_t>;
using SliceHashset = fastset::CSliceHashSet;
using Slice = fastset::Slice;
using CalcHash = fastset::CalcHash;
using SpinnedLock = fastset::SpinnedLock;

// 性能模式
#define _PROF_MODE

////////////////////////////////////////////
#ifdef _PROF_MODE
#define MAX_COUNT 100000000
#define MULTI_PASS 10
#define ASSERT_RESULT(a, b) a
#else
#define MAX_COUNT 10000000
#define MULTI_PASS 5
#define ASSERT_RESULT assert_result
#endif

#define THREADS_COUNT 64
#define SAME_DATA false

#define SLICE_BUFLEN (1 << 24)

bool assert_result(bool x, const char *msg) {
  if (!x) {
    printf("assert failed:  %s\n", msg);
  }
  return x;
}

long getTickCount() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

template <class T, class ValueT> class TestHashset {

  struct WorkerItem {
    long count;
    long result;
    long cost;
    std::thread *pthread;
  };

  ValueT _dummy;

  int *m_pBuffer{nullptr};
  int m_dataCount{0};

  std::string m_name;

public:
  TestHashset(const char *name)
      : m_name(name){

        };
  ~TestHashset() { delete[] m_pBuffer; }

  int initBuffer(bool random) {
    m_pBuffer = new int[SLICE_BUFLEN];
    // 产生随机数据
    for (int i = 0; i < SLICE_BUFLEN; i++) {
      if (random) {
        m_pBuffer[i] = rand();
      } else {
        m_pBuffer[i] = i;
      }
    }
    m_dataCount = SLICE_BUFLEN;

    return m_dataCount;
  }

  int loadTwitterData(const char *filename, int len) {
    FILE *pFile = fopen(filename, "rt");
    if (pFile == nullptr) {
      printf("file not found: %s\n", filename);
      return 0;
    }

    printf("loading %s for %d items\n", filename, len);

    m_pBuffer = new int[len];
    m_dataCount = 0;

    time_t start = getTickCount();
    int src = 0;
    char buf[256];
    for (int i = 0; i < len; i++) {
      if (fgets(buf, 256, pFile)) {
        char *value = strrchr(buf, '\t');
        if (value == nullptr) {
          value = strrchr(buf, ' ');
        }
        if (value) {
          m_pBuffer[m_dataCount++] = atoi(value + 1);
        }
      }
    }

    fclose(pFile);

    printf("total %d loaded, cost: %ld\n", m_dataCount,
           (getTickCount() - start));
    return m_dataCount;
  }

  Slice makeValue(const Slice &dummy, int i) {
    int len = i % 17 + 6;
    int off = i & (m_dataCount * sizeof(int) - 1) & 0x7fffffc; // 对齐到 8 字节
    if (off + len >= m_dataCount * sizeof(int)) {
      len = m_dataCount * sizeof(int) - off;
    }
    return Slice{len, (unsigned char *)m_pBuffer + off};
  }

  uint64_t makeValue(uint64_t dummy, int i) {
    return m_pBuffer[i % m_dataCount];
  }

  uint64_t checkSum(const Slice &slice) {
    uint64_t r = slice.len;
    int n = slice.len / sizeof(uint32_t);
    const uint32_t *buf = (uint32_t *)slice.buf;
    for (int i = 0; i < n; i++) {
      r += buf[i];
    }
    // 最后几个字节？
    n = n * sizeof(uint32_t);
    for (int i = 0; i < slice.len % sizeof(uint32_t); i++) {
      r += slice.buf[n + i];
    }
    return r;
  }

  uint64_t checkSum(uint64_t t) { return t; }

  void prof_hashset(const char *name, bool cocurrent) {
    printf("==== test %s%s...\n", m_name.c_str(), name);

    T s(cocurrent);
    T s1(cocurrent);

    time_t start = getTickCount();
    for (int i = 0; i < MAX_COUNT; i++) {
      s.add(makeValue(_dummy, i));
    }

    printf("%s add %d, %ld, cost: %ld\n", name, MAX_COUNT, s.size(),
           (getTickCount() - start));

    // s.dump_stat();

    start = getTickCount();
    long c = 0;
    for (int i = 0; i < MAX_COUNT; i++) {
#ifdef _PROF_MODE
      c += s.contains(makeValue(_dummy, i)) ? 1 : 0;
#else
      ASSERT_RESULT(s.contains(makeValue(_dummy, i)), "contain should be true");
#endif
    }
    printf("%s contains %d, %ld, cost: %ld\n", name, MAX_COUNT, c,
           (getTickCount() - start));

    ASSERT_RESULT(s.contains(makeValue(_dummy, 10)),
                  "contains(110) should be true");
    ASSERT_RESULT(!s.contains(makeValue(_dummy, -1)),
                  "contains(0) should be false");

    start = getTickCount();
    c = 0;
    int n = 0;
    for (auto it = s.begin(); it != s.end(); ++it) {
      c += checkSum(*it);
      n++;
    }
    printf("%s iterate %d, %ld, cost: %ld\n", name, n, c,
           (getTickCount() - start));

    auto it = s.begin();
    ASSERT_RESULT(it.hasNext(), "hasNext should be true");

    start = getTickCount();

    ASSERT_RESULT(s1.addAll(&s) == s.size(), "addAll should has same count");
    ASSERT_RESULT(s1.size() == s.size(), "addAll should has same count");
    printf("%s addAll %ld, cost: %ld\n", name, s1.size(),
           (getTickCount() - start));

    start = getTickCount();
    s1.clear();
    printf("%s clear cost: %ld\n", name, (getTickCount() - start));

    start = getTickCount();
    ASSERT_RESULT(s1.addAll(s.begin(), s.end()) == s.size(),
                  "addAll should has same count");
    ASSERT_RESULT(s1.size() == s.size(), "addAll should has same count");
    printf("%s addAll by iterator %ld, cost: %ld\n", name, s1.size(),
           (getTickCount() - start));

    start = getTickCount();
    s.clear();
    printf("%s clear2 cost: %ld\n", name, (getTickCount() - start));
  }

  void waitFinish(WorkerItem *sum, const WorkerItem *items, long start) {
    for (int i = 0; i < THREADS_COUNT; i++) {
      items[i].pthread->join();
      delete items[i].pthread;
      if (sum != nullptr) {
        sum->count += items[i].count;
        sum->result += items[i].result;
      }
    }
    if (sum != nullptr) {
      sum->cost = getTickCount() - start;
    }
  }

  void test_thread_one_pass(int pass, bool exclusive) {
    T s(true);

    T other(true);

    if (exclusive) {
      for (int i = 0; i < 10000; i++) {
        other.add(makeValue(_dummy, i));
      }
    }

    struct WorkerItem workers[THREADS_COUNT] = {0};
    int batch = MAX_COUNT / THREADS_COUNT;

    time_t start = getTickCount();
    for (int i = 0; i < THREADS_COUNT; i++) {
      workers[i].pthread =
          new std::thread([this, &s, &workers, i, batch, exclusive, &other] {
            long c = 0;
            long r = 0;
            int from = SAME_DATA ? 0 : i * batch;
            bool added = false;
            for (int j = 0; j < batch; j++) {
              ValueT v = makeValue(_dummy, from + j);
              if (exclusive) {
                added = s.addExclusive(v, &other);
              } else {
                added = s.add(v);
              }
              if (added) {
                r = r + this->checkSum(v);
                c++;
              }
            }
            workers[i].count = c;
            workers[i].result = r;
          });
    }
    struct WorkerItem add_ret {
      0
    };
    waitFinish(&add_ret, workers, start);

    s.debug_verify();

    start = getTickCount();
    for (int i = 0; i < THREADS_COUNT; i++) {
      workers[i].pthread =
          new std::thread([this, &s, &workers, i, batch, exclusive, &other] {
            int err = 0;
            long c = 0;
            long r = 0;
            int from = SAME_DATA ? 0 : i * batch;
            bool exist = false;
            for (int j = 0; j < batch; j++) {
              ValueT v = makeValue(_dummy, from + j);
              exist = s.contains(v);
              if (!exist && exclusive) {
                exist = other.contains(v);
              }
              if (exist) {
                r = r + this->checkSum(v);
                c++;
              } else if (++err < 10) {
                printf("error: value not found: %ld\n", checkSum(v));
              }
            }
            workers[i].count = c;
            workers[i].result = r;
          });
    }
    struct WorkerItem contain_ret {
      0
    };
    waitFinish(&contain_ret, workers, start);

#ifndef _PROF_MODE
    T s1(false); // for check dup-item
#endif

    struct WorkerItem it_ret {
      0
    };
    start = getTickCount();
    for (auto it = s.begin(); it != s.end(); ++it) {
      it_ret.result += this->checkSum(*it);
      it_ret.count++;
#ifndef _PROF_MODE
      if (!assert_result(s1.add(*it), "duplicated value found")) {
        it.dump();
        s.find(*it).dump();
      }
#endif
    }
    it_ret.cost = getTickCount() - start;

    printf("[%d] final size: %ld, add=(%ld, %ld, %ld) contains=(%ld, %ld, %ld) "
           "iterate=(%ld, %ld, %ld)\n",
           pass, s.size(), add_ret.count, add_ret.result, add_ret.cost,
           contain_ret.count, contain_ret.result, contain_ret.cost,
           it_ret.count, it_ret.result, it_ret.cost);
  }

  void test_thread_multi_pass(bool exclusive) {
    printf("==== test %sConcurrent hashset. total=%d  thread=%d  same-data=%d "
           "exclusive=%d\n",
           this->m_name.c_str(), MAX_COUNT, THREADS_COUNT, SAME_DATA,
           exclusive);

    for (int i = 0; i < MULTI_PASS; i++) {
      test_thread_one_pass(i, exclusive);
    }
  }

  void dump_values(const char *msg, const LongHashset &s) {
    if (s.size() == 0) {
      printf("%s size=%ld\n", msg, s.size());
    } else {
      printf("%s size=%ld [", msg, s.size());
      auto it = s.begin();
      printf("%ld", *it);
      while (it.hasNext()) {
        ++it;
        printf(",%ld", *it);
      }
      printf("]\n");
    }
  }

  int test_feature() {
    printf("==== test feature...\n");

    LongHashset s(true);
    LongHashset s1(true);

    int data[] = {22019760, 22019760, 22019694};
    for (int i = 0; i < sizeof(data) / sizeof(data[0]); i++) {
      s.add(data[i]);
    }

    assert_result(s.size() == 2, "size should equal to 2");

    printf("total: %ld\ntest for iterate: ", s.size());
    for (auto it = s.begin(); it != s.end(); it++) {
      printf(" %ld", *it);
    }

    printf("\ntest for isvalid: ");
    auto it = s.begin();
    while (it.isValid()) {
      printf(" %ld", *it++);
    }

    printf("\ntest for hasNext: ");
    it = s.begin();
    printf(" %ld", *it);
    while (it.hasNext()) {
      ++it;
      printf(" %ld", *it);
    }
    printf("\n");

    printf("\ntest for contains: \n");
    int a[] = {22019760, 22019694, 22019692};
    for (int i = 0; i < sizeof(a) / sizeof(a[0]); i++) {
      printf("%d:  %d\n", a[i], s.contains(a[i]));
    }

    s1.add(makeValue(_dummy, 1));
    auto itb = s1.begin();
    assert_result(itb.isValid(), "isValid should be true");
    assert_result(!itb.hasNext(), "hasNext should be false");
    assert_result(*itb++ == makeValue(_dummy, 1), "*it++ should be 1");
    assert_result(itb == s1.end(), "it should be end");

    dump_values("\ntest for addExclusive", s1);
    assert_result(!s1.addExclusive(22019760, &s),
                  "add(22019760, s) should be false");
    assert_result(s1.addExclusive(22019764, &s),
                  "add(22019764, s) should be true");
    assert_result(s1.addExclusive(1, nullptr), "add(1, null) should be true");
    dump_values("    done.", s1);

    dump_values("\ntest for remove", s1);
    assert_result(s1.remove(22019764), "remove(22019764) should be true");
    assert_result(!s1.remove(22019765), "remove(22019765) should be false");
    dump_values("    done.", s1);

    s1.add(22019764);

    dump_values("\ntest for erase", s1);
    assert_result(s1.erase(++s1.begin(), 2) == 2,
                  "erase(begin+1, 2) should return 2");
    dump_values("    done-1.", s1);
    assert_result(s1.erase(s1.begin(), 2) == 1,
                  "erase(begin, 2) should return 1");
    dump_values("    done-2.", s1);

    return 0;
  }

  void prof_unordered_set() {
    printf("==== test unordered_set...\n");

    const char *name = "std::unordered_set";
    std::unordered_set<uint64_t> s;
    std::unordered_set<uint64_t> s1;

    uint64_t dummy = 0;
    s1.emplace(makeValue(dummy, 1));

    time_t start = getTickCount();
    for (int i = 0; i < MAX_COUNT; i++) {
      s.emplace(makeValue(dummy, i));
    }

    printf("%s add %d, %ld, cost: %ld\n", name, MAX_COUNT, s.size(),
           (getTickCount() - start));

    start = getTickCount();
    long c = 0;
    for (int i = 0; i < MAX_COUNT; i++) {
      if (s.find(makeValue(dummy, i)) != s.end())
        c++;
    }
    printf("%s find %d, %ld, cost: %ld\n", name, MAX_COUNT, c,
           (getTickCount() - start));

    start = getTickCount();
    c = 0;
    int n = 0;
    for (auto it = s.begin(); it != s.end(); ++it) {
      c += *it;
      n++;
    }

    printf("%s iterate %d, %ld, cost: %ld\n", name, n, c,
           (getTickCount() - start));

    start = getTickCount();
    s1.insert(s.begin(), s.end());
    printf("%s addAll %ld, cost: %ld\n", name, s1.size(),
           (getTickCount() - start));

    start = getTickCount();
    s.clear();

    printf("%s clear cost: %ld\n", name, (getTickCount() - start));
  }

  void test_hashCode() {
    for (int i = 0; i < 20; i++) {
      uint32_t h = CalcHash::get((uint64_t)i);
      printf("%d(%x):  %x    %x\n", i, i, h, CalcHash::getShort(h));
    }

    time_t start = getTickCount();
    long h = 0;
    int t = 100000000;
    for (int i = 0; i < t; i++) {
      h += CalcHash::asNumber(makeValue(_dummy, i));
    }
    printf("makeDummy %d, %ld, cost: %ld\n", t, h, (getTickCount() - start));

    start = getTickCount();
    h = 0;
    for (int i = 0; i < t; i++) {
      h += CalcHash::get(makeValue(_dummy, i));
    }
    printf("calcHash %d, %ld, cost: %ld\n", t, h, (getTickCount() - start));

    printf("stat hashcode distribution ...\n");
    const int partCount = 64;
    int partitions[partCount]{0};
    int nextReport = 10;
    const int tableSize = 1 << 16;
    int part_0[tableSize]{0};

    LongHashset s(false);
    for (int i = 0; i < MAX_COUNT; i++) {
      uint64_t v = makeValue(_dummy, i);
      uint32_t hashCode = CalcHash::get(v);
      int partIndex = CalcHash::getShort(hashCode) % partCount;
      // 做消重
      if (s.add(v)) {
        partitions[partIndex]++;
        if (partIndex == 0) {
          part_0[hashCode % tableSize]++;
        }
      }

      if (i + 1 == nextReport) {
        nextReport *= 10;

        // 计算分区内的均衡性
        int count{0};
        int min{99999999};
        int max{0};
        int total{0};
        int square{0};
        int hist[4]{0};
        for (int k = 0; k < tableSize; k++) {
          count++;
          if (part_0[k] < 4)
            hist[part_0[k]]++;
          if (min > part_0[k])
            min = part_0[k];
          if (max < part_0[k])
            max = part_0[k];
          total += part_0[k];
          square += part_0[k] * part_0[k];
        }
        float avg = (float)total / count;
        float std = sqrt((float)square / count - avg * avg);
        printf("[%d] part_0: count=%d, total=%d, min=%d, max=%d, avg=%.3f, "
               "std=%.3f, hist=[%d",
               i, count, total, min, max, avg, std, hist[0]);

        for (int k = 1; k < 4; k++) {
          printf(",%d", hist[k]);
        }
        printf("]\n");

        for (int k = 0; k < partCount; k++) {
          printf(" %7d", partitions[k]);
          if (k % 16 == 15) {
            printf("\n");
          }
        }
        printf("\n");
      }
    }
  }

  void test_spinlock_single() {
    uint32_t lock = 0;
    struct WorkerItem workers[THREADS_COUNT] = {0};
    int start = getTickCount();
    for (int i = 0; i < THREADS_COUNT; i++) {
      workers[i].pthread = new std::thread([&lock, i] {
        char name[256]{0};
        sprintf(name, "thread_%d", i);
        for (int j = 0; j < 10; j++) {
          usleep(100000);
          SpinnedLock::doLock(&lock, name);
          usleep(100000);
          SpinnedLock::doUnlock(&lock, name);
        }
      });
    }
    waitFinish(nullptr, workers, 0);
  }

  void test_spinlock_m() {
    const int LOCKCOUNT = 10;
    uint32_t *locks = new uint32_t[LOCKCOUNT];
    struct WorkerItem workers[THREADS_COUNT] = {0};
    int start = getTickCount();
    for (int i = 0; i < THREADS_COUNT; i++) {
      workers[i].pthread = new std::thread([&locks, i] {
        char name[256]{0};
        for (int j = 0; j < 10000; j++) {
          int r = rand() % LOCKCOUNT;
          sprintf(name, "t_%d_%d", i, r);
          usleep(1000);
          SpinnedLock::doLock(locks + r, name);
          usleep(100);
          SpinnedLock::doUnlock(locks + r, name);
        }
      });
    }
    // 主线程顺序扫描
    char name[256]{0};
    for (int i = 0; i < 10000; i++) {
      int r = i % LOCKCOUNT;
      sprintf(name, "m_%d", r);
      SpinnedLock::doLock(locks + r, name);
      usleep(300);
      SpinnedLock::doUnlock(locks + r, name);
    }

    waitFinish(nullptr, workers, 0);
  }
};

using TestLongHashset = TestHashset<LongHashset, uint64_t>;
using TestSliceHashset = TestHashset<SliceHashset, Slice>;

void test_mem() {
  printf("test mem ...\n");
  for (int i = 0; i < 10000; i++) {
    LongHashset *pset = new LongHashset(true);
    for (int i = 0; i < 1000000; i++) {
      pset->add(i);
    }
    delete pset;
  }

  printf("finish\n");
  getchar();
}

int main() {
  const char *filename = "./output/e2.txt"; //
  // const char * filename = "../output/e2.txt";   // for debug

  // test_mem();

  TestLongHashset test("Long");
  // TestSliceHashset test("Slice");

  // test.test_spinlock_single();
  // test.test_spinlock_m();

  // test.initBuffer(true);    // random

  test.loadTwitterData(filename, MAX_COUNT); // for

  // test.test_hashCode();
  test.test_feature();
  test.test_thread_multi_pass(false);   // true for addExclusive test
  test.prof_hashset("HashSet", false);
  test.prof_hashset("CocurrentHashset", true);

  // #ifdef _PROF_MODE
  test.prof_unordered_set();
  // #endif
}