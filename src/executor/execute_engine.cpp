#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>

#include <chrono>

#include "parser/syntax_tree_printer.h"
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
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
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

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
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
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
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
  if(!context) {
    cout << "No database selected. Please select a database first. (0.0 sec)" << endl;
    return DB_FAILED;
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

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode astNode, ExecuteContext *execContext) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "Starting ExecuteCreateTable operation.";
#endif

    const clock_t startTime = clock();

    // Check if a database is selected
    if (current_db_.empty()) {
        cout << "No database selected. Please select a database first. ("
             << setprecision(3) << static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return DB_FAILED;
    }

    std::string tableName = astNode->child_->val_;
    std::vector<TableInfo *> existingTables;
    dbs_[current_db_]->catalog_mgr_->GetTables(existingTables);
    std::unordered_set<std::string> existingTableNames;

    for (auto& table : existingTables) {
        existingTableNames.insert(table->GetTableName());
    }

    // Check if the table already exists
    if (existingTableNames.find(tableName) != existingTableNames.end()) {
        cout << "Table " << tableName << " already exists. ("
             << setprecision(3) << static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
        return DB_TABLE_ALREADY_EXIST;
    }

    std::vector<std::string> primaryKeyNames;
    std::vector<Column*> columns;
    pSyntaxNode columnNode = astNode->child_->next_->child_;

    // Extract primary key names from the last node
    while (columnNode && columnNode->next_) {
        columnNode = columnNode->next_;
    }
    if (columnNode->val_ && std::string(columnNode->val_) == "primary keys") {
        for (pSyntaxNode keyNode = columnNode->child_; keyNode; keyNode = keyNode->next_) {
            primaryKeyNames.push_back(keyNode->val_);
        }
    }

    // Process columns
    for (columnNode = astNode->child_->next_->child_; !columnNode->val_ || (columnNode && std::string(columnNode->val_) != "primary keys"); columnNode = columnNode->next_) {
        std::string columnName = columnNode->child_->val_;
        std::string columnType = columnNode->child_->next_->val_;
        bool isUnique = (std::find(primaryKeyNames.begin(), primaryKeyNames.end(), columnName) != primaryKeyNames.end()) ||
            (columnNode->val_ && std::string(columnNode->val_) == "unique");
        bool isNullable = !isUnique;

        // Check for duplicate column names
        if (std::any_of(columns.begin(), columns.end(), [&](Column* col) { return col->GetName() == columnName; })) {
            cout << "Column name '" << columnName << "' is repeated. (" << setprecision(3)
                 << static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
            return DB_FAILED;
        }

        // Create column based on type
        Column* newColumn = nullptr;
        if (columnType == "int") {
            newColumn = new Column(columnName, kTypeInt, columns.size(), isNullable, isUnique);
        } else if (columnType == "char") {
            std::string lengthStr = columnNode->child_->next_->child_->val_;
            if (lengthStr.find('.') != std::string::npos || lengthStr.find('-') != std::string::npos) {
                cout << "Invalid length for char type. (" << setprecision(3)
                     << static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
                return DB_FAILED;
            }
            uint32_t charLength = std::stoi(lengthStr);
            newColumn = new Column(columnName, kTypeChar, charLength, columns.size(), isNullable, isUnique);
        } else if (columnType == "float") {
            newColumn = new Column(columnName, kTypeFloat, columns.size(), isNullable, isUnique);
        }
        columns.push_back(newColumn);
    }

    // Create table and indexes
    auto schema = new Schema(columns);
    Txn transaction;
    TableInfo* tableInfo = nullptr;
    dbs_[current_db_]->catalog_mgr_->CreateTable(tableName, schema, &transaction, tableInfo);

    for (auto column : columns) {
        if (column->IsUnique()) {
            IndexInfo* indexInfo = nullptr;
            std::vector<std::string> indexKeys { column->GetName() };
            dbs_[current_db_]->catalog_mgr_->CreateIndex(tableName, "index_" + column->GetName(), indexKeys, &transaction, indexInfo, "bptree");
        }
    }

    cout << "Query OK, 0 rows affected (" << setprecision(3)
         << static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC << " sec)" << endl;
    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode astNode, ExecuteContext *execContext) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "Starting ExecuteDropTable operation.";
#endif

  const clock_t startTime = clock();
  if (current_db_.empty()) {
    double timeTaken = static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC;
    cout << "No database selected. Please select a database first. (" << setprecision(3) << timeTaken << " sec)" << endl;
    return DB_FAILED;
  }

  std::string targetTableName = astNode->child_->val_;
  std::vector<TableInfo*> allTables;
  dbs_[current_db_]->catalog_mgr_->GetTables(allTables);
  std::unordered_set<std::string> existingTableNames;

  for (const auto& tableInfo : allTables) {
    existingTableNames.insert(tableInfo->GetTableName());
  }

  if (existingTableNames.find(targetTableName) == existingTableNames.end()) {
    double timeTaken = static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC;
    cout << "Table " << targetTableName << " does not exist. (" << setprecision(3) << timeTaken << " sec)" << endl;
    return DB_FAILED;
  }

  dbs_[current_db_]->catalog_mgr_->DropTable(targetTableName);
  double totalTime = static_cast<double>(clock() - startTime) / CLOCKS_PER_SEC;

  cout << "Query OK, table dropped successfully. (" << setprecision(3) << totalTime << " sec)" << endl;
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  clock_t end;
  const clock_t start = clock();
  if (current_db_.empty())
  {
    end = clock();
    cout<<"No database selected. Please select it first.("<<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<<endl;
    return DB_FAILED;
  }
  auto *writer = new ResultWriter(std::cout, false);
  if(current_db_.empty()){
    end = clock();
    cout<<"No database selected. Please select it first.("<<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<<endl;
    return DB_FAILED;
  }
  std::vector<TableInfo *>tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if(tables.empty() == true){
    end = clock();
    cout<<"No table in the "<<current_db_<<".("<<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<< endl;
    return DB_FAILED;
  }
  for (const auto table : tables){
    std::string title = "Indexes_in_"+table->GetTableName();
    int max_length;
    max_length = static_cast<int>(title.length()) + 5;
    vector<int> v_max_length;
    v_max_length.push_back(max_length);
    std::vector<IndexInfo *>indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    if(indexes.empty() == true){
      writer->Divider(v_max_length);
      writer->BeginRow(); writer->WriteCell(title, max_length); writer->EndRow();
      writer->Divider(v_max_length);
      writer->BeginRow(); writer->WriteCell(" ", max_length); writer->EndRow();
      writer->Divider(v_max_length);
      cout<<endl;
      continue;
    }
    for (const auto index : indexes)
      if (index->GetIndexName().length() > max_length)
        max_length = static_cast<int>(index->GetIndexName().length()) + 10;
    writer->Divider(v_max_length);
    writer->BeginRow(); writer->WriteCell(title, max_length); writer->EndRow();
    writer->Divider(v_max_length);
    for (const auto index : indexes){
      writer->BeginRow(); writer->WriteCell(index->GetIndexName(), max_length); writer->EndRow();
    }
    writer->Divider(v_max_length);
    cout<<endl;
  }
  end = clock();
  cout<<"Query OK. (" <<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<< endl;
  return DB_SUCCESS;
}




/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  clock_t start, end;
  start = clock();
  if (current_db_.empty())
  {
    end = clock();
    cout<<"No database selected. Please select it first.("<<setprecision(3)<<(double)(end-start)/CLOCKS_PER_SEC<<"sec)"<<endl;
    return DB_FAILED;
  }
  std::string index_name = ast->child_->val_;
  std::string table_name = ast->child_->next_->val_;
  //0. Check the validity of the table and index_name
  std::vector<TableInfo *> tables;
  TableInfo * table_info;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  bool table_exist_flag = false;
  for (const auto table: tables)
    if(table->GetTableName() == table_name)
    {
      table_exist_flag = true;
      table_info = table;
      break;
    }
  if(table_exist_flag == false){
    end = clock();
    cout<<"Table not exists."<<endl;
    return DB_FAILED;
  }
  std::vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
  for (const auto index: indexes)
    if(index->GetIndexName() == index_name)
    {
      end = clock();
      cout<<"Index "<<index_name<<" already exists"<<" in "<<table_name<<endl;
      return DB_FAILED;
    }
  std::string index_type = "btree";
  pSyntaxNode index_node = ast->child_->next_->next_ , index_node_end= ast->child_;
  //1. Judge the index type existence
  while(index_node_end->next_ != nullptr)
    index_node_end = index_node_end->next_;
  if(index_node_end->val_ != nullptr && string(index_node_end->val_) == "index type"){
    index_type = index_node_end->child_->val_;
    index_node_end = index_node_end->child_;
  }
  //2. Get the columnlist
  std::vector<std::string> column_names;
  pSyntaxNode column_node = index_node->child_;
  while(column_node != nullptr){
    column_names.emplace_back(column_node->val_);
    column_node = column_node->next_;
  }
  Txn txn;
  IndexInfo *index_info = nullptr;
  dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, column_names, &txn, index_info, index_type);
  for(TableIterator iter = table_info->GetTableHeap()->Begin(nullptr); iter != table_info->GetTableHeap()->End(); ++iter){
    Row row = *iter;
    std::vector<Column*> columns = index_info->GetIndexKeySchema()->GetColumns();
    ASSERT(columns.size() == 1, "InsertExecutor only support single column index");
    std::vector<Field> Fields;
    Fields.push_back(*(row.GetField(columns[0]->GetTableInd())));
    Row index_row(Fields);
    RowId rid = row.GetRowId();
    index_row.SetRowId(rid);

    index_info->GetIndex()->InsertEntry(index_row, rid, nullptr);
  }

  end = clock();
  cout<<"Query OK, 0 rows affected ("<<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  clock_t end;
  const clock_t start = clock();
  if (current_db_.empty()) {
    end = clock();
    cout << "No database selected. Please select it first." << endl;
    return DB_FAILED;
  }
  const std::string index_name = ast->child_->val_;
  std::vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  bool deleted_flag = false;
  for (const auto table : tables) {
    const int status = dbs_[current_db_]->catalog_mgr_->DropIndex(table->GetTableName(), index_name);
    if(status == DB_SUCCESS)
      deleted_flag = true;
  }
  if(deleted_flag == false){
    end = clock();
    cout<<"index "<<index_name<<" not exists"<<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<<endl;
    return DB_FAILED;
  }
  end = clock();
  cout<<"Query OK. (" <<setprecision(3)<< static_cast<double>(end - start) /CLOCKS_PER_SEC<<"sec)"<< endl;
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
//  auto start1 = std::chrono::steady_clock::now();
  timeval t1{}, t2{};
  gettimeofday(&t1, nullptr);
  std::string file_name = ast->child_->val_;
  ifstream in(file_name, ios::in);
  std::string line;
  while(!in.eof()){
    char cmd[1024] = {};
    // getline(in, line);
//    YY_BUFFER_STATE bp = yy_scan_string(line.c_str());
    while(in.peek() == '\n' || in.peek() == '\r')
      in.get();
    in.get(cmd, 1024, ';');
    in.get();
    int len;
    len = static_cast<int>(strlen(cmd));
    //    cout<<cmd<<endl;
    cmd[len] = ';';
    cmd[len+1] = '\0';
//    cout<<cmd<<endl;
    YY_BUFFER_STATE bp = yy_scan_string(cmd);

    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    // init parser module
    MinisqlParserInit();
    // parse
    yyparse();
    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      // Comment them out if you don't need to debug the syntax tree
    }
    this->Execute(MinisqlGetParserRootNode());
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    // quit condition
  }
//  auto end1 =  std::chrono::steady_clock::now();
//  double duration_time =
//          double((std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start1)).count());
  gettimeofday(&t2, nullptr);
  double duration;
  duration = static_cast<double>(t2.tv_sec - t1.tv_sec) + static_cast<double>(t2.tv_usec - t1.tv_usec) / 1000000.0;
  //  cout <<" Query OK.(" << setprecision(3)<<duration_time<< "sec)" << endl;
  cout <<" Query OK.(" << setprecision(3)<<duration<< "sec)" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  for(const auto& [fst, snd] : dbs_){
    delete snd;
  }
  dbs_.clear();
  return DB_QUIT;
}