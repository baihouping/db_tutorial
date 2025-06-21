//
//  main.m
//  dbTutorial
//
//  Created by bryce on 2025/6/21.
//

#import <Foundation/Foundation.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

///文件连接结构体
struct Connection_t {
    int file_descriptor;///文件描述符
};

typedef struct Connection_t Connection;

///打开文件连接
Connection* open_connection(char* filename) {
    int fd = open(filename,O_RDWR|O_CREAT,S_IWUSR|S_IREAD);
    if (fd == -1) {
        printf("Unable to open file (不能打开文件)'%s'\n", filename);
        exit(EXIT_FAILURE);
    }
    ///开辟内存
    Connection *connection = malloc(sizeof(Connection));
    connection->file_descriptor = fd;
    return connection;
}

///获取文件名
char *get_db_filename(int argc, const char * argv[]) {
    if (argc<2) {
        printf("Must supply a filename for the database(必须提供一个数据库名).\n");
        exit(EXIT_FAILURE);
    }
    return argv[1];
}

///读入缓存
struct InputBuffer_t {
    char *buffer;///读取到的内容
    size_t buffer_length;///缓存区大小
    ssize_t input_length;///读取到的大小
};

typedef struct InputBuffer_t InputBuffer;

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

int main(int argc, const char * argv[]) {
    char *db_filename = get_db_filename(argc,argv);
    Connection *connection = open_connection(db_filename);
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);
        if (strcmp(input_buffer->buffer, ".exit") == 0) {
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command(不能识别的命令) '%s'.\n", input_buffer->buffer);
        }
    }
    return 0;
}

