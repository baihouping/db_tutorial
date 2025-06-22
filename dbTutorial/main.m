//
//  main.m
//  dbTutorial
//
//  Created by bryce on 2025/6/21.
//

#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

///读入缓存
struct InputBuffer_t {
    char *buffer;///读取到的内容
    size_t buffer_length;///缓存区大小
    ssize_t input_length;///读取到的大小
};

typedef struct InputBuffer_t InputBuffer;

///执行结果枚举
enum ExecuteResult_t {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
};

typedef enum ExecuteResult_t ExecuteResult;

///元命令
enum MetaCommandResult_t {
    META_COMMAND_SUCCESS,///成功
    META_COMMAND_UNRECOGNIZED_COMMAND///不能识别的命令
};

typedef enum MetaCommandResult_t MetaCommandResult;

///准备阶段，检查命令
enum PrepareResult_t {
    PREPARE_SUCCESS,///成功
    PREPARE_SYNTAX_ERROR,///语法错误
    PREPARE_UNRECOGNIZED_STATEMENT///不能识别的语句
};

typedef enum PrepareResult_t PrepareResult;

///语句类型
enum StatementType_t {
    STATEMENT_INSERT,///增加
    STATEMENT_SELECT///筛选
};

typedef enum StatementType_t  StatementType;

///username_size
const uint32_t COLUMN_USERNAME_SIZE = 32;

///email_size
const uint32_t COLUMN_EMAIL_SIZE = 255;

///一行数据
struct Row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
};

typedef struct Row_t Row;

struct Statement_t {
    StatementType type;
    Row row_to_insert;  ///这里仅仅是插入
};

typedef struct Statement_t Statement;

///计算 id 类型的大小
const uint32_t ID_SIZE = sizeof(((Row *)0)->id);
///计算 username类型的大小
const uint32_t USERNAME_SIZE = sizeof(((Row *)0)->username);
///计算 email类型的大小
const uint32_t EMAIL_SIZE = sizeof(((Row *)0)->email);
///id 的偏移
const uint32_t ID_OFFSET = 0;
///username 的偏移
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
///email 的偏移
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
///一个 Row 结构的实际大小
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

///页大小 4k
const uint32_t PAGE_SIZE_DB = 4096;
///最大页面数
const uint32_t TABLE_MAX_PAGES = 100;
///每页的行数
const uint32_t ROWS_PER_PAGE = PAGE_SIZE_DB / ROW_SIZE;
///表的最大容量
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table_t {
    void* pages[TABLE_MAX_PAGES];
    uint32_t num_rows;
};

typedef struct Table_t Table;

void print_row(Row* row) {
  printf("Row数据:(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id),source+ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username),source+USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source+EMAIL_OFFSET, EMAIL_SIZE);
}


void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (!page) {
        ///分配内存
        page = table->pages[page_num] = malloc(PAGE_SIZE_DB);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

Table* new_table(void) {
    Table *table = malloc(sizeof(Table));
    table->num_rows = 0;
    return table;
}

///生成空的输入缓存区
InputBuffer* new_input_buffer(void){
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

///命令行提示
void print_prompt(void) {
    printf("db > ");
}

///读取缓存
void read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input(读取失败)\n");
           exit(EXIT_FAILURE);
    }
    ///忽略末尾换行
    input_buffer->input_length = bytes_read-1;
    input_buffer->buffer[bytes_read-1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(
                input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
                statement->row_to_insert.username, statement->row_to_insert.email);
            if (args_assigned < 3) {
              return PREPARE_SYNTAX_ERROR;
            }
            return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
       statement->type = STATEMENT_SELECT;
       return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if (table->num_rows>TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
  }
}

int main(int argc, const char * argv[]) {
    Table* table = new_table();
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command(不能识别的命令) '%s'\n", input_buffer->buffer);
                    continue;
                default:
                    break;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer,&statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.(语法错误)\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'.(不能识别的关键字)\n",input_buffer->buffer);
                continue;
            default:
                break;
        }
        switch (execute_statement(&statement,table)) {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
            default:
                break;
        }
    }
    return 0;
}

