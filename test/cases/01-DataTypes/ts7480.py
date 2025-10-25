from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTS7480:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def prepare_config(self):
        self.basetime =  1760687800000
        self.table_count_200 = 200
        self.table_count_500 = 200
        self.table_count_null = 200
        self.file_path = "/root/test/TS-7480/data.1"
        pass
    
    def test_ts_7480(self):
        
        self.prepare_config()
        self.create_table("st1")
        self.create_table("st2")
        self.create_table("st3")
        self.insert_data()


    def create_table(self, st_name):
        tdLog.info(f"create super table")
        tdSql.execute(f"create database if not exists test_ts7480")
        tdSql.execute(f"use test_ts7480")
        tdSql.execute(
            f"CREATE STABLE `{st_name}` (`time` TIMESTAMP, c0 int) TAGS (`t1` VARCHAR(256), `t2` VARCHAR(256));"
        )
        for i in range(self.table_count_200):
            tdSql.execute(
                f"CREATE TABLE `{st_name}_200_{i}` USING `{st_name}` TAGS ('200', 't2_{i}');"
            )
        for i in range(self.table_count_500):
            tdSql.execute(
                f"CREATE TABLE `{st_name}_500_{i}` USING `{st_name}` TAGS ('500', 't2_{i}');"
            )
        for i in range(self.table_count_null):
            tdSql.execute(
                f"CREATE TABLE `{st_name}_null_{i}` USING `{st_name}` TAGS (NULL, 't2_{i}');"
            )

    def insert_data(self):
        tdLog.info(f"insert values from csv file")
        with open(self.file_path, 'r', encoding='utf-8') as f:
            table_200_index = 0
            table_500_index = 0
            table_null_index = 0
            
            rows = 0
            batch_size = 10000
            current_table = ""
            sql_values = []
            
            stname = "st1"
            ststatus = 1
            
            for line in f:
                line = line.strip()
                if not line:
                    continue  # 跳过空行

                try:
                    flag, value = line.split(',')
                    flag = flag.strip().strip('"')  # 去掉引号
                    value = value.strip()

                    # 判断表名
                    if flag == "200":
                        table = f"{stname}_200_{table_200_index}"
                        table_200_index = (table_200_index + 1) % self.table_count_200
                    elif flag == "500":
                        table = f"{stname}_500_{table_500_index}"
                        table_500_index = (table_500_index + 1) % self.table_count_500
                    elif flag == "null" or flag == "NULL":
                        table = f"{stname}_null_{table_null_index}"
                        table_null_index = (table_null_index + 1) % self.table_count_null
                    else:  # null 或其他
                        tdLog.error(f"Unknown flag: {flag} in line: {line}")
                        continue
                    
                    # 如果是新表或者到达批次大小，执行之前的SQL
                    if (table != current_table and sql_values) or len(sql_values) >= batch_size:
                        if sql_values:
                            sql = f"INSERT INTO {current_table} VALUES {','.join(sql_values)}"
                            tdSql.execute(sql)
                            sql_values = []
                            
                            if rows > 500000 and ststatus != 3:
                                stname = "st3"
                                ststatus = 3
                                tdLog.info(f"Switching to table st3 at row {rows}")
                            elif rows > 400000 and ststatus != 2:
                                stname = "st2"
                                ststatus = 2
                                tdLog.info(f"Switching to table st2 at row {rows}")
                    
                    if rows % 10000 == 0 and rows != 0:
                        tdLog.info(f"Inserted {rows} rows so far.")
                    current_table = table
                    sql_values.append(f"({self.basetime + rows}, {value})")
                    rows += 1
                    
                except ValueError as e:
                    tdLog.error(f"Error parsing line: {line}, error: {e}")
                    continue
                except Exception as e:
                    tdLog.error(f"Error executing SQL for line: {line}, error: {e}")
                    continue
            
            # 执行最后一批数据
            if sql_values:
                sql = f"INSERT INTO {current_table} VALUES {','.join(sql_values)}"
                tdSql.execute(sql)
                
                
