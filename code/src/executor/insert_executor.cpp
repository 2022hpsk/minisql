//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  //init child_executor_
  child_executor_->Init();
  // init table_info_
  table_info_ = TableInfo::Create();
  exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,table_info_);
  // init index_info_vec_
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),index_info_vec_);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  RowId child_rid;
  Row child_row;
  if(!child_executor_->Next(&child_row,&child_rid))return false;
  for(auto it = index_info_vec_.begin();it!=index_info_vec_.end();it++){//第一次遍历，查看所有的index，是否有unique的冲突
    vector<Field> fields;
    auto keySchema = (*it)->GetIndexKeySchema();
    for(int i=0;i<keySchema->GetColumnCount();i++){
      Field* field = child_row.GetField(keySchema->GetColumn(i)->GetTableInd());//先获取列id，然后找到field，从而去建立scankey
      fields.push_back(*field);
    }
    vector<RowId>res;
    Row scankey(fields);
    if(DB_SUCCESS==(*it)->GetIndex()->ScanKey(scankey,res, exec_ctx_->GetTransaction()) ){
      //duplicate
      cout<<"Duplicate record, unique conflict when insert\n";
      return false;
    }
  }
  table_info_->GetTableHeap()->InsertTuple(child_row, nullptr);
  for(auto it = index_info_vec_.begin();it!=index_info_vec_.end();it++){//遍历，更新所有index
    auto keySchema = (*it)->GetIndexKeySchema();
    vector<Field>fields;
    for(int i=0;i<keySchema->GetColumnCount();i++){//获取索引
      Field* field = child_row.GetField(keySchema->GetColumn(i)->GetTableInd());//先获取列id，然后找到field，从而去建立key
      fields.push_back(*field);
    }
    Row key_to_insert(fields);
    (*it)->GetIndex()->InsertEntry(key_to_insert,child_row.GetRowId(),exec_ctx_->GetTransaction());
  }
  *rid = child_row.GetRowId();//返回rid
  return true;
}
