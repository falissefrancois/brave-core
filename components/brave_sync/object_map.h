/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
#ifndef BRAVE_COMPONENTS_BRAVE_SYNC_BRAVE_SYNC_OBJ_MAP_H_
#define BRAVE_COMPONENTS_BRAVE_SYNC_BRAVE_SYNC_OBJ_MAP_H_

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/bind.h"
#include "base/files/file_path.h"
#include "base/macros.h"
#include "base/sequence_checker.h"

namespace base {
class SequencedTaskRunner;
}

namespace leveldb {
  class DB;
}

namespace brave_sync {

class Bookmarks;

namespace storage {

using LoadValueCallback =
    base::OnceCallback<void(const std::string)>;
using LoadValuesCallback =
    base::OnceCallback<void(const std::vector<const std::string>)>;
using SaveValuesCallback =
    base::OnceCallback<void(const std::set<const std::string>)>;
using SaveValueCallback =
    base::OnceCallback<void(bool)>;
using DeleteValueCallback =
    base::OnceCallback<void(bool)>;
using DestroyDBCallback =
    base::OnceCallback<void()>;

// Map works in two directions:
// 1. local_id => {object_id, order, api_version}
// 2. object_id => local_id

class ObjectMap {
public:
  ObjectMap(const base::FilePath &profile_path);
  ~ObjectMap();

  // Local ids both of bookmarks and history are just int64_t and can be the same.
  enum Type {
    Unset = 0,
    Bookmark = 1,
    History = 2
  };

  enum NotSyncedRecordsOperation {
    GetItems = 0,
    AddItems = 1,
    DeleteItems = 2
  };

  void SetApiVersion(const std::string &api_version);

  void GetLocalIdByObjectId(const Type& type,
                            const std::string& object_id,
                            LoadValueCallback callback);
  void GetObjectIdByLocalId(const Type& type,
                            const std::string& local_id,
                            LoadValueCallback callback);
  void GetSpecialJSONByLocalId(const std::string& local_id,
                               LoadValueCallback callback);
  void GetOrderByObjectId(const Type &type,
                          const std::string &object_id,
                          LoadValueCallback callback);
  void GetOrderByLocalObjectId(const Type& type,
                               const std::string& local_id,
                               LoadValueCallback callback);
  void GetOrderByLocalObjectIds(const Type& type,
                                const std::vector<const std::string>& local_ids,
                                LoadValuesCallback callback);
  void SaveObjectId(const Type& type,
                    const std::string& local_id,
                    const std::string& object_id,
                    SaveValueCallback callback);
  void SaveObjectIdAndOrder(const Type &type,
                            const std::string &local_id,
                            const std::string &object_id,
                            const std::string &order,
                            SaveValueCallback callback);

  void SaveSpecialJson(const std::string &local_id,
                       const std::string &specialJSON,
                       SaveValueCallback callback);

  void UpdateOrderByLocalObjectId(const Type& type,
                                  const std::string &local_id,
                                  const std::string &order,
                                  SaveValueCallback callback);

  void CreateOrderByLocalObjectId(const Type& type,
                                  const std::string& local_id,
                                  const std::string& object_id,
                                  const std::string& order,
                                  SaveValueCallback callback);

  void SaveGetDeleteNotSyncedRecords(
      const Type& type,
      const std::string& action,
      const std::set<const std::string>& local_ids,
      const NotSyncedRecordsOperation& operation,
      SaveValuesCallback callback);

  void DeleteByLocalId(const Type& type,
                       const std::string& local_id,
                       DeleteValueCallback callback);
  void Close();
  void CloseDBHandle();
  void ResetSync(const std::string& key);
  void DestroyDB(DestroyDBCallback callback);

private:
  std::string GetLocalIdByObjectIdOnThread(const Type& type,
                                           const std::string& object_id);
  std::string GetOrderByLocalObjectIdOnThread(const Type& type,
                                              const std::string& local_id);
  bool UpdateOrderByLocalObjectIdOnThread(const Type& type,
                                          const std::string& local_id,
                                          const std::string& new_order);
  const std::vector<const std::string> GetOrderByLocalObjectIdsOnThread(
      const Type& type,
      const std::vector<const std::string>& local_ids);
  std::string GetObjectIdByLocalIdOnThread(const Type& type,
                                          const std::string& local_id);
  std::string GetOrderByObjectIdOnThread(const Type& type,
                                         const std::string& object_id);

  bool SaveObjectIdOnThread(const Type& type,
                            const std::string& local_id,
                            const std::string& object_id);
  bool SaveObjectIdAndOrderOnThread(const Type& type,
                                    const std::string& local_id,
                                    const std::string& object_id,
                                    const std::string& order);
  bool SaveObjectIdAndOrderInternal(const Type& type,
                                    const std::string& local_id,
                                    const std::string& object_id,
                                    const std::string& order);

  void DestroyDBOnThread();
  bool DeleteByLocalIdOnThread(const Type& type,
                               const std::string& local_id);

  const std::set<const std::string> SaveGetDeleteNotSyncedRecordsOnThread(
      const Type& type,
      const std::string& action,
      const std::set<const std::string>& local_ids,
      const NotSyncedRecordsOperation& operation);

  bool SaveObjectIdRawJson(const std::string& local_id,
                           const std::string& object_id_JSON,
                           const std::string& object_id);
  std::string GetRawJsonByLocalId(const std::string& local_id);

  bool GetParsedDataByLocalId(const Type& type,
                              const std::string& local_id,
                              std::string* object_id,
                              std::string* order,
                              std::string* api_version);

  bool CreateOpenDatabase();
  void TraceAll();

  void SplitRawLocalId(const std::string& raw_local_id,
                       std::string* local_id,
                       Type* read_type);
  std::string ComposeRawLocalId(const Type& type,
                                const std::string& local_id);

  std::set<const std::string> GetNotSyncedRecords(const std::string &key);
  bool SaveNotSyncedRecords(const std::string &key,
                            const std::set<const std::string>& existing_list);
  std::set<const std::string> DeserializeList(const std::string& raw);
  std::string SerializeList(const std::set<const std::string>& existing_list);

  std::string ToString(const Type& type);
  std::string ToString(const NotSyncedRecordsOperation& operation);

  base::FilePath profile_path_;
  std::unique_ptr<leveldb::DB> level_db_;
  std::string api_version_;

  scoped_refptr<base::SequencedTaskRunner> task_runner_;

  DISALLOW_COPY_AND_ASSIGN(ObjectMap);
  SEQUENCE_CHECKER(sequence_checker_);
};

} // namespace storage
} // namespace brave_sync

#endif //BRAVE_COMPONENTS_BRAVE_SYNC_BRAVE_SYNC_OBJ_MAP_H_
