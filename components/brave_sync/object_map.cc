/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
#include "brave/components/brave_sync/object_map.h"

#include <string>

#include "base/base_paths.h"
#include "base/files/file_util.h"
#include "base/files/file_path.h"
#include "base/json/json_reader.h"
#include "base/json/json_writer.h"
#include "base/path_service.h"
#include "base/sequenced_task_runner.h"
#include "base/task/post_task.h"
#include "base/threading/thread.h"
#include "base/threading/thread_checker.h"
#include "base/values.h"
#include "brave/components/brave_sync/debug.h"
#include "brave/components/brave_sync/jslib_const.h"
#include "brave/components/brave_sync/value_debug.h"
#include "chrome/browser/profiles/profile.h"
#include "chrome/common/chrome_paths.h"
#include "content/public/browser/browser_thread.h"
#include "third_party/leveldatabase/src/include/leveldb/db.h"

namespace brave_sync {
namespace storage {

static const char DB_FILE_NAME[] = "brave_sync_db";

ObjectMap::ObjectMap(const base::FilePath &profile_path) :
    task_runner_(base::CreateSequencedTaskRunnerWithTraits(
        {base::MayBlock(), base::TaskPriority::BEST_EFFORT,
          base::TaskShutdownBehavior::SKIP_ON_SHUTDOWN})) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::ObjectMap CTOR profile_path="<<profile_path;

  DETACH_FROM_SEQUENCE(sequence_checker_);

  DCHECK(!profile_path.empty());
  profile_path_ = profile_path;
}

ObjectMap::~ObjectMap() {
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::ObjectMap DTOR";
  Close();
}

void ObjectMap::SetApiVersion(const std::string &api_version) {
  DCHECK(!api_version.empty());
  DCHECK(api_version_.empty());
  api_version_ = api_version;
}

void ObjectMap::TraceAll() {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::TraceAll:-----------------------";
  leveldb::Iterator* it = level_db_->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    LOG(ERROR) << "<" << it->key().ToString() << ">: <" << it->value().ToString() << ">";
  }
  DCHECK(it->status().ok());  // Check for any errors found during the scan
  delete it;
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::TraceAll:^----------------------";
}

bool ObjectMap::CreateOpenDatabase() {
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::CreateOpenDatabase, DCHECK_CALLED_ON_VALID_SEQUENCE " << GetThreadInfoString();
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);

  if (nullptr == level_db_) {
    DCHECK(!profile_path_.empty());
    base::FilePath dbFilePath = profile_path_.Append(DB_FILE_NAME);

    LOG(ERROR) << "TAGAB ObjectMap::CreateOpenDatabase dbFilePath=" << dbFilePath;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB *level_db_raw = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, dbFilePath.value().c_str(), &level_db_raw);
    LOG(ERROR) << "TAGAB ObjectMap::CreateOpenDatabase status=" << status.ToString();
    if (!status.ok() || !level_db_raw) {
      if (level_db_raw) {
        delete level_db_raw;
      }

      LOG(ERROR) << "sync level db open error " << DB_FILE_NAME;
      return false;
    }
    level_db_.reset(level_db_raw);
    LOG(ERROR) << "TAGAB DB opened";
    TraceAll();
  }
  return true;
}

void ObjectMap::GetLocalIdByObjectId(const Type& type,
                                     const std::string& object_id,
                                     LoadValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  DCHECK(!object_id.empty());
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::GetLocalIdByObjectIdOnThread,
                      base::Unretained(this), type, object_id),
      std::move(callback));
}

std::string ObjectMap::GetLocalIdByObjectIdOnThread(const Type& type,
                                                 const std::string& object_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  if (!CreateOpenDatabase())
    return "";

  std::string value;
  leveldb::Status db_status = level_db_->Get(leveldb::ReadOptions(), object_id, &value);
  if (!db_status.ok()) {
    VLOG(1) << "TAGAB type=<" << type << ">";
    VLOG(1) << "TAGAB object_id=<" << object_id << ">";
    LOG(ERROR) << "sync level db get error " << db_status.ToString();
    return "";
  }

  std::string local_id;
  Type read_type;
  VLOG(1) << "TAGAB value="<<value;
  SplitRawLocalId(value, &local_id, &read_type);
  VLOG(1) << "TAGAB local_id="<<local_id;
  VLOG(1) << "TAGAB type="<<type;
  DCHECK(type == read_type);

  return local_id;
}

void ObjectMap::GetObjectIdByLocalId(const Type& type,
                                     const std::string& local_id,
                                     LoadValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::GetObjectIdByLocalIdOnThread,
          base::Unretained(this), type, local_id),
      std::move(callback));
}

std::string ObjectMap::GetObjectIdByLocalIdOnThread(const Type& type,
                                                  const std::string& local_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  std::string object_id;
  GetParsedDataByLocalId(type, local_id, &object_id, nullptr, nullptr);
  return object_id;
}

bool ObjectMap::GetParsedDataByLocalId(
  const Type &type,
  const std::string &local_id,
  std::string *object_id,
  std::string *order,
  std::string *api_version) {
  std::string raw_local_id = ComposeRawLocalId(type, local_id);
  LOG(ERROR) << "TAGAB ObjectMap::GetParsedDataByLocalId: raw_local_id=<" << raw_local_id << ">";
  std::string json = GetRawJsonByLocalId(raw_local_id);

  LOG(ERROR) << "TAGAB ObjectMap::GetParsedDataByLocalId: json=<" << json << ">";

  if (json.empty()) {
    LOG(ERROR) << "TAGAB ObjectMap::GetParsedDataByLocalId: json is empty, tracing all";
    TraceAll();
    return false;
  }

  // Parse JSON
  int error_code_out = 0;
  std::string error_msg_out;
  int error_line_out = 0;
  int error_column_out = 0;
  std::unique_ptr<base::Value> val = base::JSONReader::ReadAndReturnError(
    json,
    base::JSONParserOptions::JSON_PARSE_RFC,
    &error_code_out,
    &error_msg_out,
    &error_line_out,
    &error_column_out);

  LOG(ERROR) << "TAGAB ObjectMap::GetParsedDataByLocalId: val.get()="<<val.get();
  LOG(ERROR) << "TAGAB ObjectMap::GetParsedDataByLocalId: error_msg_out="<<error_msg_out;

  if (!val) {
    return false;
  }

  //LOG(ERROR) << "TAGAB ObjectMap::GetParsedDataByLocalId: val=" << brave::debug::ToPrintableString(*val);

  DCHECK(val->is_list());
  DCHECK(val->GetList().size() == 1);

  if (!val->is_list() || val->GetList().size() != 1) {
    return false;
  }

  const auto &val_entry = val->GetList().at(0);

  //std::string json = "[{\"object_id\": \"" + object_id + "\", \"order\": \"" + order + "\", \"apiVersion\": \"" + api_version_ + "\"}]";
  if (object_id) {
    *object_id = val_entry.FindKey("object_id")->GetString();
  }
  if (order) {
    *order = val_entry.FindKey("order")->GetString();
  }
  if (api_version) {
    *api_version = val_entry.FindKey("apiVersion")->GetString();
  }

  return true;
}

void ObjectMap::UpdateOrderByLocalObjectId(
    const Type& type,
    const std::string& local_id,
    const std::string& new_order,
    SaveValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  VLOG(1) << "TAGAB ObjectMap::UpdateOrderByLocalObjectId";
  VLOG(1) << "TAGAB local_id=" << local_id;
  VLOG(1) << "TAGAB new_order=" << new_order;

  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::UpdateOrderByLocalObjectIdOnThread,
                     base::Unretained(this), type, local_id, new_order),
      std::move(callback));
}

bool ObjectMap::UpdateOrderByLocalObjectIdOnThread(
    const Type& type,
    const std::string& local_id,
    const std::string& new_order) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);

  std::string object_id;
  std::string old_order;
  bool ret = GetParsedDataByLocalId(
    type,
    local_id,
    &object_id,
    &old_order,
    nullptr);

  if (!ret || object_id.empty())
    return false;

  VLOG(1) << "TAGAB object_id=" << object_id;
  VLOG(1) << "TAGAB old_order=" << old_order;

  return SaveObjectIdAndOrderInternal(type, local_id, object_id, new_order);
}

// TODO - this seems identical to SaveObjectIdAndOrder
void ObjectMap::CreateOrderByLocalObjectId(
    const Type& type,
    const std::string& local_id,
    const std::string& object_id,
    const std::string& order,
    SaveValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  DCHECK(!local_id.empty());
  DCHECK(!object_id.empty());
  DCHECK(!order.empty());

  VLOG(1) << "TAGAB ObjectMap::CreateOrderByLocalObjectId";
  VLOG(1) << "TAGAB local_id="<<local_id;
  VLOG(1) << "TAGAB object_id="<<object_id;
  VLOG(1) << "TAGAB order="<<order;

  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::SaveObjectIdAndOrderOnThread,
          base::Unretained(this), type, local_id, object_id, order),
      std::move(callback));
}

std::string ObjectMap::GetRawJsonByLocalId(const std::string &local_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  if (!CreateOpenDatabase())
    return "";

  std::string json_value;
  leveldb::Status db_status = level_db_->Get(leveldb::ReadOptions(), local_id, &json_value);

  if (!db_status.ok()) {
    LOG(ERROR) << "sync level db get error " << db_status.ToString();
  }

  return json_value;
}

bool ObjectMap::SaveObjectIdRawJson(
    const std::string& raw_local_id,
    const std::string& object_id_JSON,
    const std::string& object_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  VLOG(1) << "TAGAB brave_sync::ObjectMap::SaveObjectIdRawJson - enter";
  VLOG(1) << "TAGAB brave_sync::ObjectMap::SaveObjectIdRawJson raw_local_id="<<raw_local_id;
  VLOG(1) << "TAGAB brave_sync::ObjectMap::SaveObjectIdRawJson object_id_JSON="<<object_id_JSON;
  VLOG(1) << "TAGAB brave_sync::ObjectMap::SaveObjectIdRawJson object_id="<<object_id;

  if (!CreateOpenDatabase())
    return false;

  leveldb::Status db_status = level_db_->Put(leveldb::WriteOptions(), raw_local_id, object_id_JSON);
  if (!db_status.ok()) {
    LOG(ERROR) << "sync level db put error " << db_status.ToString();
    return false;
  }

  db_status = level_db_->Put(leveldb::WriteOptions(), object_id, raw_local_id);
  if (!db_status.ok()) {
    LOG(ERROR) << "sync level db put error " << db_status.ToString();
    return false;
  }

  return true;
}

void ObjectMap::GetSpecialJSONByLocalId(const std::string &local_id,
                                        LoadValueCallback callback) {
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::GetRawJsonByLocalId,
          base::Unretained(this), local_id),
      std::move(callback));
}

void ObjectMap::GetOrderByObjectId(const Type& type,
                                   const std::string& object_id,
                                   LoadValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::GetOrderByObjectIdOnThread,
          base::Unretained(this), type, object_id),
      std::move(callback));
}

void ObjectMap::GetOrderByLocalObjectIds(
    const Type& type,
    const std::vector<const std::string>& local_ids,
    LoadValuesCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::GetOrderByLocalObjectIdsOnThread,
          base::Unretained(this), type, local_ids),
      std::move(callback));
}

const std::vector<const std::string>
ObjectMap::GetOrderByLocalObjectIdsOnThread(
    const Type& type,
    const std::vector<const std::string>& local_ids) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  std::vector<const std::string> orders;
  for (const auto &local_id : local_ids) {
    orders.push_back(GetOrderByLocalObjectIdOnThread(type, local_id));
  }

  return orders;
}

std::string ObjectMap::GetOrderByObjectIdOnThread(const Type& type,
                                                  const std::string& object_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  std::string local_id = GetLocalIdByObjectIdOnThread(type, object_id);
  std::string order;
  std::string object_id_saved;
  GetParsedDataByLocalId(type, local_id, &object_id_saved, &order, nullptr);

  DCHECK(object_id_saved == object_id);
  return order;
}

void ObjectMap::GetOrderByLocalObjectId(
    const Type& type,
    const std::string& local_id,
    LoadValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::GetOrderByLocalObjectIdOnThread,
          base::Unretained(this), type, local_id),
      std::move(callback));
}

std::string ObjectMap::GetOrderByLocalObjectIdOnThread(
    const Type& type,
    const std::string& local_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  std::string order;
  std::string object_id_saved;
  GetParsedDataByLocalId(type, local_id, &object_id_saved, &order, nullptr);
  return order;
}


void ObjectMap::SaveObjectId(
    const Type& type,
    const std::string& local_id,
    const std::string& object_id,
    SaveValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::SaveObjectIdOnThread, base::Unretained(this),
          type, local_id, object_id),
      std::move(callback));
}

bool ObjectMap::SaveObjectIdOnThread(
    const Type& type,
    const std::string& local_id,
    const std::string& object_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  std::string json = "[{\"object_id\": \"" + object_id + "\", apiVersion\": \"" + api_version_ + "\"}]";
  return SaveObjectIdRawJson(ComposeRawLocalId(type, local_id), json, object_id);
}

void ObjectMap::SaveObjectIdAndOrder(
    const Type& type,
    const std::string& local_id,
    const std::string& object_id,
    const std::string& order,
    SaveValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::SaveObjectIdAndOrderOnThread,
          base::Unretained(this), type, local_id, object_id, order),
      std::move(callback));
}

bool ObjectMap::SaveObjectIdAndOrderOnThread(
    const Type& type,
    const std::string& local_id,
    const std::string& object_id,
    const std::string& order) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  return SaveObjectIdAndOrderInternal(type, local_id, object_id, order);
}

bool ObjectMap::SaveObjectIdAndOrderInternal(
    const Type& type,
    const std::string& local_id,
    const std::string& object_id,
    const std::string& order) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  DCHECK(!api_version_.empty());
  // TODO - this isn't safe because it doesn't escape values
  std::string json = "[{\"object_id\": \"" + object_id + "\", \"order\": \"" + order + "\", \"apiVersion\": \"" + api_version_ + "\"}]";
  return SaveObjectIdRawJson(ComposeRawLocalId(type, local_id), json, object_id);
}

void ObjectMap::SaveSpecialJson(
    const std::string& local_id,
    const std::string& special_JSON,
    SaveValueCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::SaveObjectIdRawJson,
          base::Unretained(this), local_id, special_JSON, ""),
      std::move(callback));
}

void ObjectMap::DeleteByLocalId(const Type& type,
                                const std::string& local_id,
                                DeleteValueCallback callback) {
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::DeleteByLocalIdOnThread,
          base::Unretained(this), type, local_id),
      std::move(callback));
}

bool ObjectMap::DeleteByLocalIdOnThread(const Type& type,
                                        const std::string& local_id) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);

  if (!CreateOpenDatabase())
    return false;

  std::string raw_local_id = ComposeRawLocalId(type, local_id);
  LOG(ERROR) << "TAGAB ObjectMap::DeleteByLocalId raw_local_id=" << raw_local_id;

  std::string object_id;
  bool got_parsed = GetParsedDataByLocalId(type, local_id, &object_id, nullptr, nullptr);
  LOG(ERROR) << "TAGAB ObjectMap::DeleteByLocalId object_id=" << object_id;

  leveldb::Status db_status = level_db_->Delete(leveldb::WriteOptions(), raw_local_id);
  if (!db_status.ok()) {
    LOG(ERROR) << "sync level db delete error " << db_status.ToString();
    return false;
  }
  if (got_parsed && !object_id.empty()) {
    db_status = level_db_->Delete(leveldb::WriteOptions(), object_id);
    if (!db_status.ok()) {
      LOG(ERROR) << "sync level db delete error " << db_status.ToString();
      return false;
    }
  }
  return true;
}

void ObjectMap::SaveGetDeleteNotSyncedRecords(
    const Type& type,
    const std::string& action,
    const std::set<const std::string>& local_ids,
    const NotSyncedRecordsOperation& operation,
    SaveValuesCallback callback) {
  base::PostTaskAndReplyWithResult(task_runner_.get(), FROM_HERE,
      base::BindOnce(&ObjectMap::SaveGetDeleteNotSyncedRecordsOnThread,
          base::Unretained(this), type, action, local_ids, operation),
      std::move(callback));
}

const std::set<const std::string>
ObjectMap::SaveGetDeleteNotSyncedRecordsOnThread(
    const Type& type,
    const std::string& action,
    const std::set<const std::string>& local_ids,
    const NotSyncedRecordsOperation& operation) {
  // recordType: "BOOKMARKS" | "HISTORY_SITES" | "PREFERENCES"
  // action: "0" | "1" | "2"
  std::string recordType;
  switch(type) {
    case Bookmark:
      recordType = "BOOKMARKS";
      break;
    case History:
      recordType = "HISTORY_SITES";
      break;
    default:
      NOTREACHED();
  }

  LOG(ERROR) << "TAGAB: ObjectMap::SaveGetDeleteNotSyncedRecords";
  LOG(ERROR) << "TAGAB: type=" << ToString(type);
  LOG(ERROR) << "TAGAB: action=" << action;
  LOG(ERROR) << "TAGAB: operation=" << ToString(operation);
  LOG(ERROR) << "TAGAB: local_ids=";
  for (const auto &id : local_ids) {
    LOG(ERROR) << "TAGAB:     id="<<id;
  }

  std::string key = recordType + action;
  std::set<const std::string> existing_list = GetNotSyncedRecords(key);
  LOG(ERROR) << "TAGAB: existing_list=";
  for (const auto &id : existing_list) {
    LOG(ERROR) << "TAGAB:     id="<<id;
  }

  if (operation == GetItems) {
    return existing_list;
  } else if (operation == AddItems) {
    for (const auto & id: local_ids) {
      existing_list.insert(id);
    }
  } else if (operation == DeleteItems) {
    bool list_changed = false;
    bool clear_local_db = (action == jslib_const::DELETE_RECORD); // "2"
    for (const auto & id: local_ids) {
      size_t items_removed = existing_list.erase(id);
      if (!list_changed) {
        list_changed = (items_removed != 0);
      }
      // Delete corresponding object_ids
      if (clear_local_db && (items_removed != 0)) {
        // TODO - error handling
        DeleteByLocalIdOnThread(type, id);
      }
    }
  } else {
    NOTREACHED();
  }

  if (SaveNotSyncedRecords(key, existing_list))
    return existing_list;

  return std::set<const std::string>();
}

std::set<const std::string> ObjectMap::GetNotSyncedRecords(const std::string &key) {
  // TODO - handle thread callback
  std::string raw = GetRawJsonByLocalId(key);
  LOG(ERROR) << "TAGAB ObjectMap::GetNotSyncedRecords: key="<<key;
  LOG(ERROR) << "TAGAB ObjectMap::GetNotSyncedRecords: raw="<<raw;
  std::set<const std::string> list = DeserializeList(raw);
  LOG(ERROR) << "TAGAB ObjectMap::GetNotSyncedRecords: list.size()="<<list.size();
  return list;
}

bool ObjectMap::SaveNotSyncedRecords(
    const std::string &key,
    const std::set<const std::string>& existing_list) {
  // TODO - handle thread callback
  LOG(ERROR) << "TAGAB ObjectMap::SaveNotSyncedRecords: key="<<key;
  LOG(ERROR) << "TAGAB ObjectMap::SaveNotSyncedRecords: existing_list.size()="<<existing_list.size();
  std::string raw = SerializeList(existing_list);
  LOG(ERROR) << "TAGAB ObjectMap::SaveNotSyncedRecords: raw="<<raw;
  return SaveObjectIdRawJson(key, raw, "");
}

std::set<const std::string> ObjectMap::DeserializeList(const std::string &raw) {
  // Parse JSON
  int error_code_out = 0;
  std::string error_msg_out;
  int error_line_out = 0;
  int error_column_out = 0;
  std::unique_ptr<base::Value> list = base::JSONReader::ReadAndReturnError(
    raw,
    base::JSONParserOptions::JSON_PARSE_RFC,
    &error_code_out,
    &error_msg_out,
    &error_line_out,
    &error_column_out);

  LOG(ERROR) << "TAGAB ObjectMap::DeserializeList: val.get()="<<list.get();
  LOG(ERROR) << "TAGAB ObjectMap::DeserializeList: error_msg_out="<<error_msg_out;

  if (!list) {
    return std::set<const std::string>();
  }

  std::set<const std::string> ret;
  DCHECK(list->is_list());

  for (const auto &val : list->GetList() ) {
    ret.insert(val.GetString());
  }

  return ret;
}

std::string ObjectMap::SerializeList(const std::set<const std::string> &existing_list) {
  LOG(ERROR) << "TAGAB ObjectMap::SerializeList: existing_list.size()="<<existing_list.size();
  using base::Value;
  auto list = std::make_unique<Value>(Value::Type::LIST);
  for (const auto & item: existing_list) {
    list->GetList().push_back(base::Value(item));
  }

  std::string json;
  bool result = base::JSONWriter::WriteWithOptions(
    *list,
    0,
    &json);

  LOG(ERROR) << "TAGAB ObjectMap::SerializeList: result="<<result;
  LOG(ERROR) << "TAGAB ObjectMap::SerializeList: json="<<json;
  DCHECK(result);
  return json;
}

void ObjectMap::Close() {
  task_runner_->DeleteSoon(FROM_HERE, level_db_.release());
}

void ObjectMap::CloseDBHandle() {
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::CloseDBHandle, DCHECK_CALLED_ON_VALID_SEQUENCE " << GetThreadInfoString();
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  level_db_.reset();
}

void ObjectMap::DestroyDB(DestroyDBCallback callback) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::UI);
  task_runner_->PostTaskAndReply(FROM_HERE,
      base::BindOnce(&ObjectMap::DestroyDBOnThread, base::Unretained(this)),
      std::move(callback));
}

void ObjectMap::DestroyDBOnThread() {
  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::DestroyDB, DCHECK_CALLED_ON_VALID_SEQUENCE " << GetThreadInfoString();
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  DCHECK(!profile_path_.empty());

  LOG(ERROR) << "TAGAB brave_sync::ObjectMap::ResetObjects";
  CloseDBHandle();

  base::FilePath dbFilePath = profile_path_.Append(DB_FILE_NAME);
  LOG(ERROR) << "TAGAB ResetObjects dbFilePath=" << dbFilePath;

  leveldb::Status db_status = leveldb::DestroyDB(dbFilePath.value(), leveldb::Options());
  if (!db_status.ok()) {
    LOG(ERROR) << "sync level db destroy error " << db_status.ToString();
    DCHECK(false);
  }
}

void ObjectMap::ResetSync(const std::string& key) {
  DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);
  if (!CreateOpenDatabase()) {
    // TODO - need better error handling here
    return;
  }
  level_db_->Delete(leveldb::WriteOptions(), key);
}

void ObjectMap::SplitRawLocalId(const std::string& raw_local_id,
                                std::string* local_id,
                                Type* read_type) {
  // is this actually possible? Status should be IsNotFound if there is no value
  if (raw_local_id.empty()) {
    *local_id = "";
    *read_type = Unset;
    return;
  }

  char object_type_char = raw_local_id.at(0);
  switch (object_type_char) {
    case 'b':
      *read_type = Bookmark;
      local_id->assign(raw_local_id.begin() + 1, raw_local_id.end());
    break;
    case 'h':
      *read_type = History;
      local_id->assign(raw_local_id.begin() + 1, raw_local_id.end());
    break;
    default:
      *read_type = Unset;
      *local_id = raw_local_id;
    break;
  }
}

std::string ObjectMap::ComposeRawLocalId(const ObjectMap::Type &type, const std::string &local_id) {
  switch (type) {
    case Unset:
      return local_id;
    break;
    case Bookmark:
      return 'b' + local_id;
    break;
    case History:
      return 'h' + local_id;
    break;
    default:
      NOTREACHED();
  }
}

std::string ObjectMap::ToString(const Type &type) {
  switch (type) {
    case Unset:
      return "Unset";
    break;
    case Bookmark:
      return "Bookmark";
    break;
    case History:
      return "History";
    break;
    default:
      NOTREACHED();
  }
}

std::string ObjectMap::ToString(const NotSyncedRecordsOperation &operation) {
  switch (operation) {
    case GetItems:
      return "GetItems";
    break;
    case AddItems:
      return "AddItems";
    break;
    case DeleteItems:
      return "DeleteItems";
    break;
    default:
      NOTREACHED();
  }
}

} // namespace storage
} // namespace brave_sync
