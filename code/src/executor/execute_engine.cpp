#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());

  tcount_+=duration_time;//for exec_file!!
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  if (itr_ == cmd_vec_.end() && exec_) {
    exec_ = false;
    //LOG(INFO) << "Exec_file use " << tcount_ << " ms.";
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)!=dbs_.end())
    return DB_ALREADY_EXIST;
  dbs_[db_name]=new DBStorageEngine(db_name);
  cout << "Database " << db_name << " successfully created." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)==dbs_.end())
    return DB_NOT_EXIST;
  cout << "Database " << db_name << " successfully dropped." << endl;
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if(current_db_==db_name)
    current_db_="";
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  std::stringstream sstream;
  ResultWriter Writer(sstream);
  vector<int>width_vec;
  width_vec.push_back(10);
  //先放一个10
  for(auto pair : dbs_)
    width_vec[0]=max((int)(pair.first.length()),width_vec[0]);

  Writer.Divider(width_vec);
  Writer.BeginRow();
  Writer.WriteHeaderCell("Database",width_vec[0]);//长度为width_vec【0】
  Writer.EndRow();
  Writer.Divider(width_vec);

  for(auto pair:dbs_){
    Writer.BeginRow();
    Writer.WriteCell(pair.first,width_vec[0]);
    Writer.EndRow();
  }
  Writer.Divider(width_vec);
  cout<<Writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)==dbs_.end())
    return DB_NOT_EXIST;
  cout << "Use database " << db_name << endl;
  current_db_=db_name;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  //未选当前使用db
  if(current_db_.empty())
    return DB_FAILED;
  string Head="Tables at database "+current_db_;

  vector<TableInfo*>table_info_vec;
  table_info_vec.clear();
  context->GetCatalog()->GetTables(table_info_vec);

  std::stringstream sstream;
  ResultWriter Writer(sstream);
  vector<int>width_vec;
  width_vec.push_back(Head.length());
  //把表头的长度放进去
  for(auto itr:table_info_vec)
    width_vec[0]=max((int)itr->GetTableName().length(),width_vec[0]);
  Writer.Divider(width_vec);
  Writer.BeginRow();
  Writer.WriteHeaderCell(Head,width_vec[0]);
  Writer.EndRow();
  Writer.Divider(width_vec);

  for(auto itr:table_info_vec){
    Writer.BeginRow();
    Writer.WriteCell(itr->GetTableName(),width_vec[0]);
    Writer.EndRow();
  }
  Writer.Divider(width_vec);
  std::cout<<Writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  //未选当前使用db
  if(current_db_.empty())
    return DB_FAILED;

  string table_name(ast->child_->val_);
  vector<string>primary_key_vec;
  vector<string>unique_key_vec;
  auto cur_node=ast->child_->next_->child_;
  while(cur_node!= nullptr){
    //得到主键循环
    if(cur_node->type_==kNodeColumnList&&string(cur_node->val_)=="primary keys"){
      auto pri_node=cur_node->child_;
      while(pri_node!= nullptr){
        primary_key_vec.push_back(string(pri_node->val_));
        pri_node=pri_node->next_;
      }
    }
    cur_node=cur_node->next_;
  }
  cur_node=ast->child_->next_->child_;//回到原来的node

  // 获取columns
  uint32_t index_cnt=0;
  vector<Column*>columns_vec;
  while(cur_node!= nullptr&&cur_node->type_==kNodeColumnDefinition){
    //是否是unique？
    bool is_unique=(cur_node->val_!=nullptr&&string(cur_node->val_)=="unique");
    auto child_node=cur_node->child_;
    string col_name(child_node->val_);
    Column *column_new;
    string type_str(child_node->next_->val_);
    if(type_str=="int")
      column_new=new Column(col_name,kTypeInt,index_cnt,true,is_unique);
    if(type_str=="float")
      column_new=new Column(col_name,kTypeFloat,index_cnt,true,is_unique);
    if(type_str=="char"){
      string num(child_node->next_->child_->val_);
      //一个一个看是不是数字,不然stoi（num）会报错
      for(auto digit:num)
        if(!isdigit(digit))
          return DB_FAILED;
      //不能是负数！！！
      if(stoi(num)<0)
        return DB_FAILED;
      column_new=new Column(col_name,kTypeChar,stoi(num),index_cnt,true,is_unique);
    }
    if(is_unique){
      unique_key_vec.push_back(col_name);
    }
    columns_vec.push_back(column_new);
    cur_node=cur_node->next_;
    index_cnt++;
  }
  Schema *schema=new Schema(columns_vec);
  TableInfo *table_info;
  auto err=context->GetCatalog()->CreateTable(table_name,schema,context->GetTransaction(),table_info);
  if(err!=DB_SUCCESS)
    return err;

  if(primary_key_vec.size()!=0) {
    IndexInfo *index_info;
    err=context->GetCatalog()->CreateIndex(table_name, "AUTO_PK_IDX_ON_"+table_name, primary_key_vec, context->GetTransaction(), index_info, "bptree");
    if(err!=DB_SUCCESS)
      return err;
  }
  for(auto cur_unique_key:unique_key_vec){
    string name = "UNIQUE_"+cur_unique_key;
    name+="_ON_"+table_name;
    IndexInfo *index_info;
    err=context->GetCatalog()->CreateIndex(table_name,name,unique_key_vec, context->GetTransaction(), index_info, "bptree");
    if(err!=DB_SUCCESS)
      return err;
  }
  cout<<"Table "<<table_name<<" successfully created."<<endl;
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty())
    return DB_FAILED;
  string table_name(ast->child_->val_);

  auto err=context->GetCatalog()->DropTable(table_name);
  if(err!=DB_SUCCESS)
    return err;

  vector<IndexInfo*>index_info_vec_;
  context->GetCatalog()->GetTableIndexes(table_name,index_info_vec_);
  for(auto index_info_:index_info_vec_){
    err=context->GetCatalog()->DropIndex(table_name,index_info_->GetIndexName());
    if(err!=DB_SUCCESS)
      return err;
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty())
    return DB_FAILED;

  dberr_t err;
  vector<TableInfo*>table_info_vec_;{
    err=context->GetCatalog()->GetTables(table_info_vec_);
    if(err!=DB_SUCCESS)
      return err;
  }

  vector<IndexInfo*>index_info_vec_;
  for(auto table_info_:table_info_vec_){
    err=context->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),index_info_vec_);
    if(err!=DB_SUCCESS)
      return err;
  }

  std::stringstream sstream;
  ResultWriter Writer(sstream);
  vector<int>width_vec_;
  //先放5（index的len）
  width_vec_.push_back(5);
  for(auto index_info_:index_info_vec_)
    width_vec_[0]=max((int)index_info_->GetIndexName().length(),width_vec_[0]);
  Writer.Divider(width_vec_);
  Writer.BeginRow();
  Writer.WriteHeaderCell("Index",width_vec_[0]);
  Writer.EndRow();
  Writer.Divider(width_vec_);

  for(auto index_info_:index_info_vec_){
    Writer.BeginRow();
    Writer.WriteCell(index_info_->GetIndexName(),width_vec_[0]);
    Writer.EndRow();
  }

  Writer.Divider(width_vec_);
  std::cout<<Writer.stream_.rdbuf();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif

  string index_name(ast->child_->val_);
  string table_name(ast->child_->next_->val_);
  vector<string> column_names;
  for (auto column_itr = ast->child_->next_->next_->child_; column_itr != nullptr; column_itr = column_itr->next_)
    column_names.push_back(column_itr->val_);
  string type="bptree";
  if (ast->child_->next_->next_->next_ != nullptr)
    type = string(ast->child_->next_->next_->next_->child_->val_);


  TableInfo * table_info_;
  IndexInfo * index_info_;
  auto err = context->GetCatalog()->GetTable(table_name, table_info_);
  if (err != DB_SUCCESS)
    return err;
  err = context->GetCatalog()->CreateIndex(table_name,index_name,column_names,context->GetTransaction(),index_info_,type);
  if (err != DB_SUCCESS)
    return err;

  for (auto row =  table_info_->GetTableHeap()->Begin(context->GetTransaction()); row != table_info_->GetTableHeap()->End(); ++row) {
    auto rid = (*row).GetRowId();
    vector<Field> field_vec_;
    for (auto column : index_info_->GetIndexKeySchema()->GetColumns())
      field_vec_.push_back(*(*row).GetField(column->GetTableInd()));
    Row key(field_vec_);
    err = index_info_->GetIndex()->InsertEntry(key,rid,context->GetTransaction());
    if (err != DB_SUCCESS)
      return err;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty())
    return DB_FAILED;
  string index_name(ast->child_->val_);
  vector<TableInfo*>table_info_vec_;
  auto res=DB_INDEX_NOT_FOUND;
  context->GetCatalog()->GetTables(table_info_vec_);
  for(auto table_info_:table_info_vec_){
    dberr_t err=context->GetCatalog()->DropIndex(table_info_->GetTableName(),index_name);
    if(err!=DB_SUCCESS)
      return err;
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name(ast->child_->val_);
  ifstream fp(file_name, ios::in);
  tcount_ = 0;
  exec_= true;
  cmd_vec_.resize(0);
  int i = 0;
  char char0;
  char cmd[1024];//1024Byte buffer size
  memset(cmd, 0, 1024);
  if (fp.is_open()) {
    while (fp.get(char0)) {
      cmd[i] = char0;
      i++;
      if (char0 == ';') {
        //这一行结束了。读入\n
        fp.get(char0);
        cmd_vec_.emplace_back(cmd);

        YY_BUFFER_STATE bp = yy_scan_string(cmd);
        if (bp == nullptr) {
          LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
          exit(1);
        }
        yy_switch_to_buffer(bp);
        MinisqlParserInit();
        yyparse();
        if (MinisqlParserGetError())
          cout<< MinisqlParserGetErrorMessage()<<endl;
        auto res = Execute(MinisqlGetParserRootNode());
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();
        ExecuteInformation(res);
        if (res == DB_QUIT) {
          break;
        }
        memset(cmd, 0, 1024);
        i = 0;//i reset , cmd reset
      }
    }
    fp.close();
  }
  itr_ = cmd_vec_.begin();
  return DB_SUCCESS;
  //时间的输出最后再exec函数中
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  current_db_ = "";
  return DB_QUIT;
}
