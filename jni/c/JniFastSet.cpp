
#include "../../src/fasthashset.h"

using LongFastset = fastset::CSimpleHashSet<int64_t>;
using LongFastset_iterator = fastset::CSimpleHashSet<int64_t>::iterator;

using Slice = fastset::Slice;
using SliceFastset = fastset::CSliceHashSet;
using SliceFastset_iterator = fastset::CSliceHashSet::iterator;

extern "C" {
#include "com_baidu_hugegraph_util_collection_JniBytesSet.h"
#include "com_baidu_hugegraph_util_collection_JniBytesSetIterator.h"
#include "com_baidu_hugegraph_util_collection_JniLongSet.h"
#include "com_baidu_hugegraph_util_collection_JniLongSetIterator.h"
}

/////////////////////////////////////////////////////////////////////////
// JNILongSet
/////////////////////////////////////////////////////////////////////////

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    init
 * Signature: (Z)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_init(JNIEnv *env,
                                                         jobject obj,
                                                         jboolean cocurrent, 
                                                         jint partitionBits, 
                                                         jint capacityBits) {
  LongFastset *set = new LongFastset(cocurrent, partitionBits, capacityBits);
  return (jlong)set;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    iterator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_iterator(JNIEnv *env,
                                                             jobject obj,
                                                             jlong ptr) {
  LongFastset *set = (LongFastset *)ptr;
  LongFastset_iterator *it = new LongFastset_iterator(set->begin());
  return (jlong)it;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    add
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_add(JNIEnv *env,
                                                        jobject obj, jlong ptr,
                                                        jlong value) {
  LongFastset *set = (LongFastset *)ptr;
  return set->add(value);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    addAll
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_addAll(JNIEnv *env,
                                                           jobject obj,
                                                           jlong ptr,
                                                           jlong src) {
  LongFastset *set = (LongFastset *)ptr;
  LongFastset *srcset = (LongFastset *)src;
  return (jlong)set->addAll(srcset->begin(), srcset->end());
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    addExclusive
 * Signature: (JJJ)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_addExclusive(
    JNIEnv *env, jobject obj, jlong ptr, jlong value, jlong other) {
  LongFastset *set = (LongFastset *)ptr;
  LongFastset *otherset = (LongFastset *)other;
  return (jlong)set->addExclusive(value, otherset);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    contains
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_contains(JNIEnv *env,
                                                             jobject obj,
                                                             jlong ptr,
                                                             jlong value) {
  LongFastset *set = (LongFastset *)ptr;
  return set->contains(value);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    remove
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_remove(JNIEnv *env,
                                                             jobject obj,
                                                             jlong ptr,
                                                             jlong value) {
  LongFastset *set = (LongFastset *)ptr;
  return set->remove(value);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    erase
 * Signature: (JJ)Z
 */
JNIEXPORT jint JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_erase(JNIEnv *env,
                                                             jobject obj,
                                                             jlong ptr,
                                                             jlong iter,
                                                             jint count) {
  LongFastset *set = (LongFastset *)ptr;
  LongFastset_iterator &it = *(LongFastset_iterator *)iter;
  return set->erase(it, count);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    size
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_size(JNIEnv *env,
                                                         jobject obj,
                                                         jlong ptr) {
  LongFastset *set = (LongFastset *)ptr;
  return set->size();
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    clear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_clear(JNIEnv *env,
                                                          jobject obj,
                                                          jlong ptr) {
  LongFastset *set = (LongFastset *)ptr;
  set->clear();
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSet
 * Method:    deleteNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSet_deleteNative(JNIEnv *env,
                                                                 jobject obj,
                                                                 jlong ptr) {
  LongFastset *set = (LongFastset *)ptr;
  delete set;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSetIterator
 * Method:    hasNext
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSetIterator_hasNext(JNIEnv *env,
                                                                    jobject obj,
                                                                    jlong ptr) {
  LongFastset_iterator &it = *(LongFastset_iterator *)ptr;
  // hasNext 应当检查当前项是否有效
  return it.isValid();
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSetIterator
 * Method:    next
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSetIterator_next(JNIEnv *env,
                                                                 jobject obj,
                                                                 jlong ptr) {
  LongFastset_iterator &it = *(LongFastset_iterator *)ptr;
  return *it++;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniLongSetIterator
 * Method:    deleteNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_baidu_hugegraph_util_collection_JniLongSetIterator_deleteNative(
    JNIEnv *env, jobject obj, jlong ptr) {
  LongFastset_iterator *it = (LongFastset_iterator *)ptr;
  delete it;
}

/////////////////////////////////////////////////////////////////////////
// JNIBytesSet
/////////////////////////////////////////////////////////////////////////

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    init
 * Signature: (Z)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_init(JNIEnv *env,
                                                          jobject obj,
                                                          jboolean cocurrent,
                                                          jint partitionBits, 
                                                          jint capacityBits) {
  SliceFastset *set = new SliceFastset(cocurrent, partitionBits, capacityBits);
  return (jlong)set;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    iterator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_iterator(JNIEnv *env,
                                                              jobject obj,
                                                              jlong ptr) {
  SliceFastset *set = (SliceFastset *)ptr;
  SliceFastset_iterator *it = new SliceFastset_iterator(set->begin());
  return (jlong)it;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    add
 * Signature: (J[B)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_add(JNIEnv *env,
                                                         jobject obj, jlong ptr,
                                                         jbyteArray value) {
  SliceFastset *set = (SliceFastset *)ptr;
  int len = env->GetArrayLength(value);
  jbyte *p = env->GetByteArrayElements(value, NULL);
  Slice slice{len, (unsigned char *)p};
  bool ret = set->add(slice);
  env->ReleaseByteArrayElements(value, p, 0);
  return ret;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    addAll
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_addAll(JNIEnv *env,
                                                            jobject obj,
                                                            jlong ptr,
                                                            jlong src) {
  SliceFastset *set = (SliceFastset *)ptr;
  SliceFastset *srcset = (SliceFastset *)src;
  return (jlong)set->addAll(srcset);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    addExclusive
 * Signature: (J[BJ)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_addExclusive(
    JNIEnv *env, jobject obj, jlong ptr, jbyteArray value, jlong other) {
  SliceFastset *set = (SliceFastset *)ptr;
  SliceFastset *otherset = (SliceFastset *)other;

  int len = env->GetArrayLength(value);
  jbyte *p = env->GetByteArrayElements(value, NULL);
  Slice slice{len, (unsigned char *)p};
  bool ret = set->addExclusive(slice, otherset);
  env->ReleaseByteArrayElements(value, p, 0);
  return ret;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    contains
 * Signature: (J[B)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_contains(
    JNIEnv *env, jobject obj, jlong ptr, jbyteArray value) {
  SliceFastset *set = (SliceFastset *)ptr;
  // 避免内存复制
  int len = env->GetArrayLength(value);
  jbyte *p = env->GetByteArrayElements(value, NULL);
  Slice slice{len, (unsigned char *)p};
  bool ret = set->contains(slice);
  env->ReleaseByteArrayElements(value, p, 0);
  return ret;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    remove
 * Signature: (J[B)Z
 */

JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_remove(
    JNIEnv *env, jobject obj, jlong ptr, jbyteArray value) {
  SliceFastset *set = (SliceFastset *)ptr;
  // 避免内存复制
  int len = env->GetArrayLength(value);
  jbyte *p = env->GetByteArrayElements(value, NULL);
  Slice slice{len, (unsigned char *)p};
  bool ret = set->remove(slice);
  env->ReleaseByteArrayElements(value, p, 0);
  return ret;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    erase
 * Signature: (JJ)Z
 */
JNIEXPORT jint JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_erase(JNIEnv *env,
                                                             jobject obj,
                                                             jlong ptr,
                                                             jlong iter,
                                                             jint count) {
  SliceFastset *set = (SliceFastset *)ptr;
  SliceFastset_iterator &it = *(SliceFastset_iterator *)iter;
  return set->erase(it, count);
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    size
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_size(JNIEnv *env,
                                                          jobject obj,
                                                          jlong ptr) {
  SliceFastset *set = (SliceFastset *)ptr;
  return set->size();
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    clear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_clear(JNIEnv *env,
                                                           jobject obj,
                                                           jlong ptr) {
  SliceFastset *set = (SliceFastset *)ptr;
  set->clear();
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSet
 * Method:    deleteNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSet_deleteNative(JNIEnv *env,
                                                                  jobject obj,
                                                                  jlong ptr) {
  SliceFastset *set = (SliceFastset *)ptr;
  delete set;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSetIterator
 * Method:    hasNext
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSetIterator_hasNext(
    JNIEnv *env, jobject obj, jlong ptr) {
  SliceFastset_iterator &it = *(SliceFastset_iterator *)ptr;
  // hasNext 应当检查当前项是否有效
  return it.isValid();
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSetIterator
 * Method:    next
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSetIterator_next(JNIEnv *env,
                                                                  jobject obj,
                                                                  jlong ptr) {
  SliceFastset_iterator &it = *(SliceFastset_iterator *)ptr;
  Slice slice = *it++;
  jbyteArray result = env->NewByteArray(slice.len);
  env->SetByteArrayRegion(result, 0, slice.len, (jbyte *)slice.buf);
  return result;
}

/*
 * Class:     com_baidu_hugegraph_util_collection_JniBytesSetIterator
 * Method:    deleteNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_baidu_hugegraph_util_collection_JniBytesSetIterator_deleteNative(
    JNIEnv *env, jobject obj, jlong ptr) {
  SliceFastset_iterator *it = (SliceFastset_iterator *)ptr;
  delete it;
}