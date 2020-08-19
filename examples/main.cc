#include <iostream>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"

using namespace std;

void check(leveldb::Status sts) {
  if (!sts.ok())
    cerr << sts.ToString() << endl;
}

void dump(leveldb::DB *db, leveldb::ReadOptions readOpts) {
  leveldb::Iterator *it = db->NewIterator(readOpts);
  for (it->SeekToFirst(); it->Valid(); it->Next())
    cout << it->key().ToString() << ":" << it->value().ToString() << endl;
  delete it;
  cout << endl;
}

// g++ -fno-rtt
class TwoPartComparator : public leveldb::Comparator {
 public:
  int Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
    int a1, a2, b1, b2;
    parseKey(a, &a1, &a2);
    parseKey(b, &b1, &b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }
  const char *Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string *, const leveldb::Slice &) const {}
  void FindShortSuccessor(std::string *) const {}

 private:
  static void parseKey(const leveldb::Slice &v, int *v1, int *v2) {
    int n = stoi(v.ToString());
    n %= 100;
    *v1 = n / 10;
    *v2 = n % 10;
  }
};

int main() {
  leveldb::WriteOptions dftWOpts;
  leveldb::ReadOptions dftROpts;

  // 创建并打开 DB
  leveldb::DB *db;
  leveldb::Options openOpts;
  openOpts.create_if_missing = true;
  leveldb::Status sts = leveldb::DB::Open(openOpts, "/tmp/db1", &db);
  check(sts);

  // 读写
  string k1 = "K1", v = "VAL", k2 = "K2";
  sts = db->Put(leveldb::WriteOptions(), k1, v);
  if (sts.ok()) {
    leveldb::WriteBatch wb; // 原子更新
    wb.Delete(k1);
    wb.Put(k2, v);
    leveldb::WriteOptions writeOpts;
    writeOpts.sync = true;
    check(db->Write(writeOpts, &wb));
  }

  // 并发
  // sts = leveldb::DB::Open(openOpts, "/tmp/db1", &db2);
  // check(sts); // IO error: lock /tmp/db1/LOCK: already held by process // 文件锁保护 db dir
  // Get 线程安全，Put/WriteBatch/Iterator 需多线程加互斥锁

  // 迭代
  dump(db, dftROpts);

  // 快照
  leveldb::ReadOptions readOpts;
  readOpts.snapshot = db->GetSnapshot();
  check(db->Put(dftWOpts, "K3", "V3"));
  dump(db, readOpts); // No K3:V3
  db->ReleaseSnapshot(readOpts.snapshot);

  // Slice
  leveldb::Slice slice = "s1";
  {
    string s2 = "s2";
    slice = s2;
  } // s2 已被析构
  cout << slice.ToString() << endl; // 未定义


  // 比较器
  leveldb::DB *cdb;
  TwoPartComparator tpCmp;
  openOpts.comparator = &tpCmp;
  check(leveldb::DB::Open(openOpts, "/tmp/db2", &cdb));
  for (auto s:{"12", "3", "2", "21"}) // 12 2 21 3
    check(cdb->Put(dftWOpts, s, s));
  dump(cdb, dftROpts);

  // 关闭 DB
  delete db;
}

