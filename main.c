/*
Created by Changjong Kim on 2022/10/27.
Modify by Changjong kim on 2023/04/07

/*
 
*/

#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>

#define _CRT_SECURE_NO_WARNINGS

#define MAX_DATA 1000

// Linux Config
#define remote_ip "165.194.35.5"
#define remote_dir "/home/syslab/skim/"
#define remote_pw "sys5583"
#define remote_port "2209"
#define remote_localhost "syslab"

// Linux Config > File_name
#define cpu_file_name1 "cpu_usage1"
#define memory_file_name1 "memory_usage1"
#define network_file_name1 "network_usage1"
#define disk_file_name1 "disk_usage1"


#define cpu_file_name2 "cpu_usage2"
#define memory_file_name2 "memory_usage2"
#define network_file_name2 "network_usage2"
#define disk_file_name2 "disk_usage2"

// Head Window Local Config
#define local_dir "/Users/alstj/OneDrive/Desktop/CSV_Collect/"
#define sshpass "/usr/local/bin/sshpass"
#define DB_DIR "/Users/alstj/OneDrive/Desktop/"
#define DB_NAME "testdb"

// Compute Window Config
#define window_file_name1 "DataCollector01"
#define window_file_name2 "DataCollector02"



// 구조체 생성(데이터를 배열안에 위치) > 결과를 구조체로 받아오고, insert 시에도 사용됨.
struct data {
    char s[MAX_DATA][1048];
};

// csv 파일 내의 공백 제거
void remove_spaces(char* s) {
    const char* d = s;
    do {
        while (*d == ' ') {
            ++d;
        }
    }
    while (*s++ == *d++);
}


// csv 파일 내의 세미클론 제거 및 복사한 문자열
void getfield(char* line, struct data *d, int end_idx) {
    int idx = 0;
    char *token = strtok(line, ";,");

    do {
        strcpy(d->s[idx++], token); //&& strcpy(d->s[idx], token1);
    }
    while ( idx != end_idx && (token = strtok(NULL, ";,")));
    
}


/*
 SCP로 파일 가져오기 (Linux)
 sshpass를 사용했는데, 윈도우에서 먹히는 지 확인 필요
 *** Window(Head)에 sshpass 유틸리티 깔면 되는듯?
 *** Linux(Compute)에 sysstat_auto.sh 수행해서 5분마다 File auto 생성
*/

/*
void sshpass_func(void) {
    char cmd[1000];
    for (int i=0; i<4; i++) {
        if(i == 0) {
            sprintf(cmd, "%s -p %s scp -P %s %s@%s:%s%s.csv %s", sshpass, remote_pw, remote_port, remote_localhost, remote_ip, remote_dir, cpu_file_name, local_dir);
            printf("SCP Command : %s\n", cmd);
            system(cmd);
            printf("File Copy Done\n");
        }
        if(i == 1) {
            sprintf(cmd, "%s -p %s scp -P %s %s@%s:%s%s.csv %s", sshpass, remote_pw, remote_port, remote_localhost, remote_ip, remote_dir, memory_file_name, local_dir);
            printf("SCP Command : %s\n", cmd);
            system(cmd);
            printf("File Copy Done\n");
        }
        if(i == 2) {
            sprintf(cmd, "%s -p %s scp -P %s %s@%s:%s%s.csv %s", sshpass, remote_pw, remote_port, remote_localhost, remote_ip, remote_dir, disk_file_name, local_dir);
            printf("SCP Command : %s\n", cmd);
            system(cmd);
            printf("File Copy Done\n");
        }
        if(i == 3) {
            sprintf(cmd, "%s -p %s scp -P %s %s@%s:%s%s.csv %s", sshpass, remote_pw, remote_port, remote_localhost, remote_ip, remote_dir, network_file_name, local_dir);
            printf("SCP Command : %s\n", cmd);
            system(cmd);
            printf("File Copy Done\n");
        }
    }
}

*/

/*
 
 FIFO 등 Application 수행 시, 어떤 Compute에서 리소스가 제일 튀는 지 등등 보려면? flag 필요한데
 우선 사용자가 돌렸다는 가정하에, 돌린 시간과 Application 명을 scanf로 받고
 받은 값을 쿼리에 입혀서 출력하면 될거같은데
 
 너무 별론데..?
  
 */

void user_config(void){
    char flag;
    
    char start_time[50];
    char end_time[50];
    char app[50];
    
    printf("시간 지정 리소스 통합 확인 (Y/N) : ");
    scanf("%[^\n]s\n", &flag);
    
    rewind(stdin);
    
    if((flag == 'y') || (flag == 'Y')){
        
        printf("사용한 application 입력해주세요 : ");
        scanf("%[^\n]c\n", app);
        rewind(stdin);
        
        printf("리소스 수집 START_TIME 입력해주세요(ex.2022-10-10 15:00:00): ");
        scanf("%[^\n]c\n", start_time);
        rewind(stdin);
        
        printf("리소스 수집 END_TIME 입력해주세요(ex.2022-10-10 15:00:00): ");
        scanf("%[^\n]c\n", end_time);
        rewind(stdin);
        

        printf("---------------Calculation--------------\n");
    
        sqlite3 *db;
        char *err_msg = 0;

        //DB Open
        char db_open_cmd[500];
        sprintf(db_open_cmd, "%s%s.db", DB_DIR, DB_NAME);
        int rc = sqlite3_open(db_open_cmd, &db);

        if (rc != SQLITE_OK) { //오류처리구문
            fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
        }

        if (rc != SQLITE_OK ) {
            fprintf(stderr, "SQL error: %s\n", err_msg);
            sqlite3_free(err_msg);
            sqlite3_close(db);
        }
        
        char* buf =
        "DROP TABLE IF EXISTS COMPUTE_USAGE_TIME_CONFIG;"
        "CREATE TABLE COMPUTE_USAGE_TIME_CONFIG (NODE STRING, APPLICATION STRING, START_TIME datetime, END_TIME datetime, MAX_CPU_USER_RATE DOUBLE, MAX_CPU_SYSTEM_RATE DOUBLE, MAX_CPU_IDLE_RATE DOUBLE, AVG_CPU_USER_RATE DOUBLE, AVG_CPU_SYSTEM_RATE DOUBLE, AVG_CPU_IDLE_RATE DOUBLE, AVG_MEM_KB DOUBLE, AVG_DISK_READ_KB_SEC DOUBLE, AVG_DISK_WRITE_KB_SEC DOUBLE, AVG_NET_RX_KB_SEC DOUBLE, AVG_NET_TX_KB_SEC DOUBLE);"
        ;

        char* buf1 =
        sqlite3_mprintf("INSERT INTO COMPUTE_USAGE_TIME_CONFIG SELECT REPLACE(host_name, 'syslab-pm1725b', 'Linux_compute') AS node, %Q, MIN(timestamp) AS START_TIME, MAX(timestamp) AS END_TIME,ROUND(MAX(user),2) AS MAX_CPU_USER, ROUND(MAX(system),2) AS MAX_CPU_SYSTEM, ROUND(MAX(idle),2) AS MAX_CPU_IDLE,ROUND(AVG(user),2) AS AVG_CPU_USER, ROUND(AVG(system),2) AS AVG_CPU_SYSTEM, ROUND(AVG(idle),2) AS AVG_CPU_IDLE,ROUND(AVG(kbavail),2) AS AVG_MEM, ROUND(AVG(rkbSec),2) AS AVG_DISK_R_SEC, ROUND(AVG(wkbSec),2) AS AVG_DISK_W_SEC,ROUND(AVG(rxkbSec),2) AS AVG_NET_RX_SEC, ROUND(AVG(txkbSec),2) AS AVG_NET_TX_SEC FROM LINUX_USAGE WHERE timestamp BETWEEN %Q AND %Q;", app, start_time, end_time)
        ;
        
        char* buf2 =
        sqlite3_mprintf("INSERT INTO COMPUTE_USAGE_TIME_CONFIG SELECT REPLACE(host_name, 'temp', 'Window_compute') AS node, %Q, MIN(timestamp) AS START_TIME, MAX(timestamp) AS END_TIME,ROUND(MAX(CPU_usertime),2) AS MAX_CPU_USER, ROUND(MAX(CPU_privilegedtime),2) AS MAX_CPU_SYSTEM, ROUND(MAX(CPU_idletime),2) AS MAX_CPU_IDLE, ROUND(AVG(CPU_usertime),2) AS AVG_CPU_USER, ROUND(AVG(CPU_privilegedtime),2) AS AVG_CPU_SYSTEM, ROUND(AVG(CPU_idletime),2) AS AVG_CPU_IDLE, ROUND(AVG(memory_availble_Kbytes),2) AS AVG_MEM, ROUND(AVG(Disk_Read_Byte),2) AS AVG_DISK_R_SEC, ROUND(AVG(Disk_Write_Byte),2) AS AVG_DISK_W_SEC, ROUND(AVG(Network_Recv_Bytes1),2) AS AVG_NET_RX_SEC, ROUND(AVG(Network_sent_Bytes1),2) AS AVG_NET_TX_SEC FROM Window_usage WHERE timestamp BETWEEN %Q AND %Q;", app, start_time, end_time);
        
        
        printf("query : %s\n", buf);
        printf("query : %s\n", buf1);
        printf("query : %s\n", buf2);
        
        rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
        rc = sqlite3_exec(db, buf1, 0, 0, &err_msg);
        rc = sqlite3_exec(db, buf2, 0, 0, &err_msg);
        
        printf("------------Calculation Done-------------\n");
        
    }
    else if ((flag == 'n') || (flag == 'N')) {
        printf("프로그램 종료\n");
        exit(0);
    }
    else
        printf("프로그램 종료\n");
        exit(0);

}


int main(void) {
    
    printf("File Copy & Resource Calculation Start\n");
    printf("----------------------------------------\n");
    
    // scp > sshpass
    sshpass_func();
    
    printf("-----------SQLITE DB OPEN AND DATA PARSING ----------\n");
   
    sqlite3 *db;
    char *err_msg = 0;

    //DB Open
    char db_open_cmd[500];
    sprintf(db_open_cmd, "%s%s.db", DB_DIR, DB_NAME);
    int rc = sqlite3_open(db_open_cmd, &db);

    // DB Error
    if (rc != SQLITE_OK) { //오류처리구문
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }
    // DB 내의 compute_linux_table 생성
    char *sql =
        "DROP TABLE IF EXISTS cpu_compute_linux1;"
        "DROP TABLE IF EXISTS memory_compute_linux1;"
        "DROP TABLE IF EXISTS network_compute_linux1;"
        "DROP TABLE IF EXISTS disk_compute_linux1;"
        "DROP TABLE IF EXISTS cpu_compute_linux2;"
        "DROP TABLE IF EXISTS memory_compute_linux2;"
        "DROP TABLE IF EXISTS network_compute_linux2;"
        "DROP TABLE IF EXISTS disk_compute_linux2;"
    
        "CREATE TABLE cpu_compute_linux1(host_name string, interval int, timestamp datetime, cpu double, user double, nice double, system double, iowait double, steal double, idle double);"
        "CREATE TABLE memory_compute_linux1(host_name string, interval int, timestamp datetime, kbmemfree double, kbavail double, kbmemused double, memused double, kbbuffers double, kbcached double, kbcommit double, commitrate double, kbactive double, kbinact double, kbdirty double);"
        "CREATE TABLE network_compute_linux1(host_name string, interval int, timestamp datetime, iface string, rxpckSec double, txpckSec double, rxkbSec double, txkbSec double, rxcmpSec double, txcmpSec double, rxmcstSec double, ifutilRate double);"
        "CREATE TABLE disk_compute_linux1(host_name string, interval int, timestamp datetime, DEV string, tps double, rkbSec double, wkbSec double, dkbSec double, areq_sz double, aqu_sz double, await double, utilrate double);"
        "CREATE TABLE cpu_compute_linux2(host_name string, interval int, timestamp datetime, cpu double, user double, nice double, system double, iowait double, steal double, idle double);"
        "CREATE TABLE memory_compute_linux2(host_name string, interval int, timestamp datetime, kbmemfree double, kbavail double, kbmemused double, memused double, kbbuffers double, kbcached double, kbcommit double, commitrate double, kbactive double, kbinact double, kbdirty double);"
        "CREATE TABLE network_compute_linux2(host_name string, interval int, timestamp datetime, iface string, rxpckSec double, txpckSec double, rxkbSec double, txkbSec double, rxcmpSec double, txcmpSec double, rxmcstSec double, ifutilRate double);"
        "CREATE TABLE disk_compute_linux2(host_name string, interval int, timestamp datetime, DEV string, tps double, rkbSec double, wkbSec double, dkbSec double, areq_sz double, aqu_sz double, await double, utilrate double);"
        

        "DROP TABLE IF EXISTS WINDOW_USAGE1;"
        "DROP TABLE IF EXISTS WINDOW_USAGE2;"
        "CREATE TABLE WINDOW_USAGE1(host_name string, timestamp datetime, Memory_availble_Kbytes double, Network_Recv_Bytes1 double, Network_Recv_Bytes2 double, Network_Recv_Bytes3 double, Network_sent_bytes1 double, Network_sent_bytes2 double, Network_sent_bytes3 double, Disk_Read_Byte double, Disk_Write_Byte double, CPU_idletime double, CPU_Privilegedtime double, CPU_usertime double);"
        "CREATE TABLE WINDOW_USAGE2(host_name string, timestamp datetime, Memory_availble_Kbytes double, Network_Recv_Bytes1 double, Network_Recv_Bytes2 double, Network_Recv_Bytes3 double, Network_sent_bytes1 double, Network_sent_bytes2 double, Network_sent_bytes3 double, Disk_Read_Byte double, Disk_Write_Byte double, CPU_idletime double, CPU_Privilegedtime double, CPU_usertime double);"
        ;


    printf("query : %s\n", sql);
    rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
   
    /*
       DB Insert & Table Generate
       * State 별 수행 동작
       State 0 ~ 6
       0: Window_info / 1:Linux_cpu / 2:Linux_memory / 3:Linux_network
       4:Linux_disk / 5:Linux_info (Create table / Inner Join) / 6: Compute_usage_info (Create table / Insert table)
       * 가져올 컬럼 정보 (total 14개(Time(start-end) / host 포함)
           > Cpu : usertime / idletime / systemtime (MAX / AVG)
           > Memory : kbmem (AVG)
           > Network : recv / sent Bytes (AVG)
           > Disk : Read / Write Bytes (AVG)
     
     */
    
    for(int state=0; state<=15; state++) {
        char file_open_cmd[500]; //File open 변수
        
        // Window1
        if (state == 0) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, window_file_name1);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048]; //한 라인의 문자열 최대개수
            fgets(line, sizeof(line), stream); // 헤더 날리기
            
            while(fgets(line, sizeof(line), stream)){ //라인 내의 문자 최대개수만큼 파일 읽기
                struct data d; //구조체
                remove_spaces(line);
                char *tmp = strdup(line); //메모리 할당
                getfield(tmp, &d, MAX_DATA);
                
                for (int i= 0; i <= 12; i++) {
                    //  printf("[%s]\n", d.s[i]);
                    //  printf("[%c]\n", d.s[i][strlen(d.s[i])-2]);
                    for (int j = 2; j <= strlen(d.s[i]); j++) {
                        if (d.s[i][j] == '\"') {
                            d.s[i][j] = '\0';
                        }
                    }
                }
                
            
                // 데이터 insert
                char* buf = sqlite3_mprintf("INSERT INTO WINDOW_USAGE1 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", window_file_name1, d.s[0]+1, d.s[1]+2, d.s[2]+2, d.s[3]+2, d.s[4]+2, d.s[5]+2, d.s[6]+2, d.s[7]+2, d.s[8]+2, d.s[9]+2, d.s[10]+2, d.s[11]+2, d.s[12]+2);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
                //메모리 해제 필요 (leak 발생 가능성)
            }
            fclose(stream); // file close
        }

        //window2
        if (state == 1) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, window_file_name2);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048]; //한 라인의 문자열 최대개수
            fgets(line, sizeof(line), stream); // 헤더 날리기
            
            while(fgets(line, sizeof(line), stream)){ //라인 내의 문자 최대개수만큼 파일 읽기
                struct data d; //구조체
                remove_spaces(line);
                char *tmp = strdup(line); //메모리 할당
                getfield(tmp, &d, MAX_DATA);
                
                for (int i= 0; i <= 12; i++) {
                    //  printf("[%s]\n", d.s[i]);
                    //  printf("[%c]\n", d.s[i][strlen(d.s[i])-2]);
                    for (int j = 2; j <= strlen(d.s[i]); j++) {
                        if (d.s[i][j] == '\"') {
                            d.s[i][j] = '\0';
                        }
                    }
                }
                
            
                // 데이터 insert
                char* buf = sqlite3_mprintf("INSERT INTO WINDOW_USAGE2 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", window_file_name2, d.s[0]+1, d.s[1]+2, d.s[2]+2, d.s[3]+2, d.s[4]+2, d.s[5]+2, d.s[6]+2, d.s[7]+2, d.s[8]+2, d.s[9]+2, d.s[10]+2, d.s[11]+2, d.s[12]+2);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
                //메모리 해제 필요 (leak 발생 가능성)
            }
            fclose(stream); // file close
        }



        //CPU1
        if(state == 3) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, cpu_file_name1);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);

            while (fgets(line, sizeof(line), stream)){
                struct data d;
                remove_spaces(line);
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
                
                char* buf = sqlite3_mprintf("INSERT INTO cpu_compute_linux1 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
                
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }


        //CPU2
        if(state == 4) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, cpu_file_name2);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);

            while (fgets(line, sizeof(line), stream)){
                struct data d;
                remove_spaces(line);
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
                
                char* buf = sqlite3_mprintf("INSERT INTO cpu_compute_linux2 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
                
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }


        // Network1
        if(state == 5) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, network_file_name1);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);
            
            while (fgets(line, sizeof(line), stream)) {
                struct data d;
                remove_spaces(line);
           
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
                

                char* buf = sqlite3_mprintf("INSERT INTO network_compute_linux1 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9], d.s[10], d.s[11]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
                
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }

        // Network2
        if(state == 6) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, network_file_name2);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);
            
            while (fgets(line, sizeof(line), stream)) {
                struct data d;
                remove_spaces(line);
           
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
                

                char* buf = sqlite3_mprintf("INSERT INTO network_compute_linux2 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9], d.s[10], d.s[11]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
                
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }
        
        //Memory1
        if(state == 7) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, memory_file_name1);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);
            
            while (fgets(line, sizeof(line), stream)) {
                struct data d;
                remove_spaces(line);
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
               
                char* buf = sqlite3_mprintf("INSERT INTO memory_compute_linux1 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9], d.s[10], d.s[11], d.s[12], d.s[13]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
              
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }

        //memory 2
        if(state == 8) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, memory_file_name2);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);
            
            while (fgets(line, sizeof(line), stream)) {
                struct data d;
                remove_spaces(line);
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
               
                char* buf = sqlite3_mprintf("INSERT INTO memory_compute_linux2 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9], d.s[10], d.s[11], d.s[12], d.s[13]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
              
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }


        // Disk1
        if (state == 9) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, disk_file_name1);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);
      
            while (fgets(line, sizeof(line), stream)) {
                struct data d;
                remove_spaces(line);
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
         
                char* buf = sqlite3_mprintf("INSERT INTO disk_compute_linux1 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9], d.s[10], d.s[11]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
             
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }

        // Disk2
        if (state == 10) {
            sprintf(file_open_cmd, "%s%s.csv", local_dir, disk_file_name2);
            FILE* stream = fopen(file_open_cmd, "r");
            char line[1048];
            fgets(line, sizeof(line), stream);
      
            while (fgets(line, sizeof(line), stream)) {
                struct data d;
                remove_spaces(line);
                char *tmp = strdup(line);
                getfield(tmp, &d, MAX_DATA);
         
                char* buf = sqlite3_mprintf("INSERT INTO disk_compute_linux2 VALUES(%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q,%Q);", d.s[0], d.s[1], d.s[2], d.s[3], d.s[4], d.s[5], d.s[6], d.s[7], d.s[8], d.s[9], d.s[10], d.s[11]);
                rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
                printf("Query : %s\n", buf);
                free(tmp);
             
            }
            fclose(stream);
            remove(file_open_cmd); //file delete
        }        


        // Window 테이블(타임스탬프 포멧변경) & Linux 통합 관리 테이블 생성
        if (state == 11) {
            char *sql =
            // Window timestamp 데이터 포멧 변경
            
            "DROP TABLE IF EXISTS linux_1;"
            "DROP TABLE IF EXISTS linux_2;"
            "DROP TABLE IF EXISTS linux_3;"
            "DROP TABLE IF EXISTS linux_4;"
            "DROP TABLE IF EXISTS LINUX_USAGE1;"
            "DROP TABLE IF EXISTS LINUX_USAGE2;"
            
            "CREATE TABLE linux_1 AS SELECT a.host_name, REPLACE(a.timestamp,' UTC','') as timestamp, a.rkbSec, a.wkbSec, b.kbavail, b.memused FROM disk_compute_linux AS A inner join memory_compute_linux AS B on A.timestamp = B.timestamp;"
            "CREATE TABLE linux_2 AS SELECT c.host_name, REPLACE(c.timestamp,' UTC','') as timestamp, c.user, c.system, c.idle, d.rxkbSec, d.txkbSec FROM cpu_compute_linux AS C inner join network_compute_linux AS D on C.timestamp = D.timestamp;"
            "CREATE TABLE linux_3 AS SELECT a.host_name, REPLACE(a.timestamp,' UTC','') as timestamp, a.rkbSec, a.wkbSec, b.kbavail, b.memused FROM disk_compute_linux AS A inner join memory_compute_linux AS B on A.timestamp = B.timestamp;"
            "CREATE TABLE linux_4 AS SELECT c.host_name, REPLACE(c.timestamp,' UTC','') as timestamp, c.user, c.system, c.idle, d.rxkbSec, d.txkbSec FROM cpu_compute_linux AS C inner join network_compute_linux AS D on C.timestamp = D.timestamp;"
            "CREATE TABLE LINUX_USAGE1 AS SELECT a.host_name, DISTINCT(REPLACE(a.timestamp,' UTC','')) as timestamp, a.rkbSec, a.wkbSec, a.kbavail, a.memused, b.user, b.system, b.idle, b.rxkbSec, b.txkbSec FROM linux_1 as A INNER JOIN linux_2 as B ON A.timestamp = B.timestamp;"
            "CREATE TABLE LINUX_USAGE2 AS SELECT a.host_name, DISTINCT(REPLACE(a.timestamp,' UTC','')) as timestamp, a.rkbSec, a.wkbSec, a.kbavail, a.memused, b.user, b.system, b.idle, b.rxkbSec, b.txkbSec FROM linux_1 as A INNER JOIN linux_2 as B ON A.timestamp = B.timestamp;"
            
            ;
            
            printf("query : %s\n", sql);
            rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
        }
        // Window 통합 테이블 > Timestamp 포멧 변경
        // Compute 통합 테이블 생성
        // Round 처리로 소수점 절삭 처리 | 각 리소스 별 계산 결과(AVG/MAX) Insert 작업
        if (state == 12) {
            
            // Timestamp 타입 변경(Update)
            char* buf = sqlite3_mprintf("update WINDOW_USAGE1 set timestamp = substr(timestamp, 7, 4) || '-' || substr(timestamp,1,2)  || '-' || substr(timestamp, 4,2) || ' '  || substr(timestamp, 12, 8);");
            char* buf1 = sqlite3_mprintf("update WINDOW_USAGE2 set timestamp = substr(timestamp, 7, 4) || '-' || substr(timestamp,1,2)  || '-' || substr(timestamp, 4,2) || ' '  || substr(timestamp, 12, 8);");
            
            
            char *buf2 = 
            "DROP TABLE linux_1;"
            "DROP TABLE linux_2;"
            "DROP TABLE linux_3;"
            "DROP TABLE linux_4;"
            
            "DROP TABLE IF EXISTS COMPUTE_USAGE_INFO;"
            "CREATE TABLE COMPUTE_USAGE_INFO (NODE STRING, START_TIME datetime, END_TIME datetime, MAX_CPU_USER_RATE DOUBLE, MAX_CPU_SYSTEM_RATE DOUBLE, MAX_CPU_IDLE_RATE DOUBLE, AVG_CPU_USER_RATE DOUBLE, AVG_CPU_SYSTEM_RATE DOUBLE, AVG_CPU_IDLE_RATE DOUBLE, AVG_MEM_KB DOUBLE, AVG_DISK_READ_KB_SEC DOUBLE, AVG_DISK_WRITE_KB_SEC DOUBLE, AVG_NET_RX_KB_SEC DOUBLE, AVG_NET_TX_KB_SEC DOUBLE);"
            
            //Window
            "INSERT INTO COMPUTE_USAGE_INFO SELECT REPLACE(host_name, 'temp', 'Window_compute'), MIN(timestamp) AS START_TIME, MAX(timestamp) AS END_TIME,ROUND(MAX(CPU_usertime),2) AS MAX_CPU_USER, ROUND(MAX(CPU_privilegedtime),2) AS MAX_CPU_SYSTEM, ROUND(MAX(CPU_idletime),2) AS MAX_CPU_IDLE, ROUND(AVG(CPU_usertime),2) AS AVG_CPU_USER, ROUND(AVG(CPU_privilegedtime),2) AS AVG_CPU_SYSTEM, ROUND(AVG(CPU_idletime),2) AS AVG_CPU_IDLE, ROUND(AVG(memory_availble_Kbytes),2) AS AVG_MEM, ROUND(AVG(Disk_Read_Byte),2) AS AVG_DISK_R_SEC, ROUND(AVG(Disk_Write_Byte),2) AS AVG_DISK_W_SEC, ROUND(AVG(Network_Recv_Bytes1),2) AS AVG_NET_RX_SEC, ROUND(AVG(Network_sent_Bytes1),2) AS AVG_NET_TX_SEC FROM Window_usage1;"
            "INSERT INTO COMPUTE_USAGE_INFO SELECT REPLACE(host_name, 'temp', 'Window_compute'), MIN(timestamp) AS START_TIME, MAX(timestamp) AS END_TIME,ROUND(MAX(CPU_usertime),2) AS MAX_CPU_USER, ROUND(MAX(CPU_privilegedtime),2) AS MAX_CPU_SYSTEM, ROUND(MAX(CPU_idletime),2) AS MAX_CPU_IDLE, ROUND(AVG(CPU_usertime),2) AS AVG_CPU_USER, ROUND(AVG(CPU_privilegedtime),2) AS AVG_CPU_SYSTEM, ROUND(AVG(CPU_idletime),2) AS AVG_CPU_IDLE, ROUND(AVG(memory_availble_Kbytes),2) AS AVG_MEM, ROUND(AVG(Disk_Read_Byte),2) AS AVG_DISK_R_SEC, ROUND(AVG(Disk_Write_Byte),2) AS AVG_DISK_W_SEC, ROUND(AVG(Network_Recv_Bytes1),2) AS AVG_NET_RX_SEC, ROUND(AVG(Network_sent_Bytes1),2) AS AVG_NET_TX_SEC FROM Window_usage2;"
            
            
            //Linux
            "INSERT INTO COMPUTE_USAGE_INFO SELECT REPLACE(host_name, 'syslab-pm1725b', 'Linux_compute') AS node, MIN(timestamp) AS START_TIME, MAX(timestamp) AS END_TIME,ROUND(MAX(user),2) AS MAX_CPU_USER, ROUND(MAX(system),2) AS MAX_CPU_SYSTEM, ROUND(MAX(idle),2) AS MAX_CPU_IDLE,ROUND(AVG(user),2) AS AVG_CPU_USER, ROUND(AVG(system),2) AS AVG_CPU_SYSTEM, ROUND(AVG(idle),2) AS AVG_CPU_IDLE,ROUND(AVG(kbavail),2) AS AVG_MEM, ROUND(AVG(rkbSec),2) AS AVG_DISK_R_SEC, ROUND(AVG(wkbSec),2) AS AVG_DISK_W_SEC,ROUND(AVG(rxkbSec),2) AS AVG_NET_RX_SEC, ROUND(AVG(txkbSec),2) AS AVG_NET_TX_SEC FROM LINUX_USAGE1;"
            "INSERT INTO COMPUTE_USAGE_INFO SELECT REPLACE(host_name, 'syslab-pm1725b', 'Linux_compute') AS node, MIN(timestamp) AS START_TIME, MAX(timestamp) AS END_TIME,ROUND(MAX(user),2) AS MAX_CPU_USER, ROUND(MAX(system),2) AS MAX_CPU_SYSTEM, ROUND(MAX(idle),2) AS MAX_CPU_IDLE,ROUND(AVG(user),2) AS AVG_CPU_USER, ROUND(AVG(system),2) AS AVG_CPU_SYSTEM, ROUND(AVG(idle),2) AS AVG_CPU_IDLE,ROUND(AVG(kbavail),2) AS AVG_MEM, ROUND(AVG(rkbSec),2) AS AVG_DISK_R_SEC, ROUND(AVG(wkbSec),2) AS AVG_DISK_W_SEC,ROUND(AVG(rxkbSec),2) AS AVG_NET_RX_SEC, ROUND(AVG(txkbSec),2) AS AVG_NET_TX_SEC FROM LINUX_USAGE2;"

             ;
            
            rc = sqlite3_exec(db, buf, 0, 0, &err_msg);
            rc = sqlite3_exec(db, buf1, 0, 0, &err_msg);
            rc = sqlite3_exec(db, buf2, 0, 0, &err_msg);
            printf("query : %s\n", buf);
            printf("query : %s\n", buf1);
            printf("query : %s\n", buf2);
        }
 

    }
         
    sqlite3_close(db);
    printf("--------------------------------------\n");
    printf("File Upload & Resouce Calculation Done\n");
    printf("--------------------------------------\n");
    
    
    user_config();
    
}





