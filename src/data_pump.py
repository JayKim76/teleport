import logging
import concurrent.futures
from contextlib import contextmanager

logger = logging.getLogger('Teleport.DataPump')

class TeleportDataPump:
    def __init__(self, source_config, target_config, target_schema):
        """
        source_config: AS-IS Oracle 접속 정보
        target_config: TO-BE Oracle 접속 정보
        target_schema: 데이터를 넣을 대상 스키마
        """
        self.source_config = source_config
        self.target_config = target_config
        self.target_schema = target_schema

    def _create_connection(self, config):
        """멀티프로세싱 환경을 위한 개별 커넥션 생성 (oracledb)"""
        import oracledb
        dsn = f"{config['host']}:{config['port']}/{config['service_name']}"
        return oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)

    def _fetch_column_names(self, table_name):
        """복사할 테이블의 컬럼 목록을 가져옴"""
        query = f"SELECT column_name FROM all_tab_columns WHERE table_name = '{table_name}' AND owner = '{self.target_schema}' ORDER BY column_id"
        try:
            with self._create_connection(self.source_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to fetch columns for {table_name}: {e}")
            return []

    def _worker_copy_chunk(self, task):
        """단일 청크 데이터를 읽어서 타겟으로 바로 밀어넣는 작업자(Worker) 함수"""
        table_name = task['table_name']
        columns = task['columns']
        split_method = task['split_method']
        
        # 청크 조건 생성
        where_clause = ""
        if split_method == 'PK_RANGE':
            where_clause = f"WHERE {task['split_column']} >= {task['start_val']} AND {task['split_column']} < {task['end_val']}"
        elif split_method == 'ROWID_RANGE':
            # ROWID를 범위로 자르는 특수 쿼리 (실제로는 ORA_HASH 등을 조합하여 구현)
            where_clause = f"WHERE MOD(ORA_HASH(ROWID), {task['total_chunks']}) = {task['chunk_id']}"

        select_query = f"SELECT {', '.join(columns)} FROM {self.target_schema}.{table_name} {where_clause}"
        
        bind_vars = ', '.join([':' + str(i+1) for i in range(len(columns))])
        insert_query = f"INSERT INTO {self.target_schema}.{table_name} ({', '.join(columns)}) VALUES ({bind_vars})"
        
        rows_copied = 0
        try:
            # Source에서 읽어서 Target으로 쓰기 (한 작업자당 독립된 Connection 사용)
            with self._create_connection(self.source_config) as src_conn, \
                 self._create_connection(self.target_config) as tgt_conn:
                
                with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
                    # Target DB 최적화: 배열 삽입(Array DML) 준비
                    tgt_cur.setinputsizes(*([None] * len(columns)))
                    
                    src_cur.execute(select_query)
                    
                    # 메모리 오버플로우 방지를 위해 Fetchmany로 일정 단위씩 가져옴
                    batch_size = 10000
                    while True:
                        rows = src_cur.fetchmany(batch_size)
                        if not rows:
                            break
                        
                        # 타겟에 Bulk Insert (executemany가 오라클에서 매우 빠름)
                        tgt_cur.executemany(insert_query, rows)
                        rows_copied += len(rows)
                    
                    tgt_conn.commit()
            
            logger.info(f"[Worker] {table_name} - Chunk {task.get('chunk_id', 1)}/{task.get('total_chunks', 1)} completed. ({rows_copied} rows)")
            return True, rows_copied
            
        except Exception as e:
            logger.error(f"[Worker] Failed chunk for {table_name}: {e}")
            return False, 0

    def execute_migration(self, master_plan):
        """마스터 플랜을 받아 병렬 이관을 지휘하는 메인 펌프 엔진"""
        logger.info("Starting Teleport Data Pump Execution...")
        total_rows_migrated = 0
        
        # 테이블별로 루프
        for plan in master_plan:
            table_name = plan['table_name']
            threads_assigned = plan['threads_assigned']
            
            columns = self._fetch_column_names(table_name)
            if not columns:
                logger.warning(f"Skipping {table_name}: No columns found.")
                continue
                
            logger.info(f"Starting transfer for {table_name} using {threads_assigned} threads...")
            
            # 작업(Task) 목록 생성
            tasks = []
            if plan['split_method'] == 'FULL_TABLE':
                tasks.append({'table_name': table_name, 'columns': columns, 'split_method': 'FULL_TABLE'})
            else:
                # 쪼개기 로직 (예시로 MOD 함수 기반 분할)
                for i in range(plan['chunks']):
                    tasks.append({
                        'table_name': table_name,
                        'columns': columns,
                        'split_method': plan['split_method'],
                        'split_column': plan['split_column'],
                        'chunk_id': i,
                        'total_chunks': plan['chunks']
                    })
            
            # 병렬 처리 (ThreadPoolExecutor 사용)
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads_assigned) as executor:
                results = list(executor.map(self._worker_copy_chunk, tasks))
            
            # 테이블 단위 결과 집계
            table_success = all([r[0] for r in results])
            table_rows = sum([r[1] for r in results])
            total_rows_migrated += table_rows
            
            if table_success:
                logger.info(f"Successfully migrated table {table_name}. Total rows: {table_rows}")
            else:
                logger.error(f"Errors occurred during migration of table {table_name}.")
                
        logger.info(f"Teleport Execution Complete. Total Rows Migrated: {total_rows_migrated}")
        return total_rows_migrated
