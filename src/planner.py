import logging
import math

logger = logging.getLogger('Teleport.Planner')

class MigrationPlanner:
    def __init__(self, db_connector, target_schema, max_threads_per_table=4, target_chunk_size=100000):
        """
        db_connector: DBConnector 인스턴스 (Oracle)
        target_schema: 분석할 스키마(OWNER)
        max_threads_per_table: 한 테이블당 띄울 수 있는 최대 병렬 쓰레드 수
        target_chunk_size: 한 번에 Select/Insert 할 이상적인 Row 단위
        """
        self.db_connector = db_connector
        self.target_schema = target_schema.upper()
        self.max_threads = max_threads_per_table
        self.target_chunk_size = target_chunk_size

    def _get_primary_key(self, table_name):
        """테이블의 PK 컬럼을 찾아서 반환. 없으면 ROWID 기반을 위해 None 반환"""
        query = f"""
            SELECT cols.column_name
            FROM all_constraints cons
            INNER JOIN all_cons_columns cols 
              ON cons.constraint_name = cols.constraint_name 
             AND cons.owner = cols.owner
            WHERE cons.table_name = '{table_name}' 
              AND cons.owner = '{self.target_schema}'
              AND cons.constraint_type = 'P'
        """
        try:
            with self.db_connector.get_oracle_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    result = cur.fetchone()
                    if result:
                        return result[0]
                    return None
        except Exception as e:
            logger.error(f"Failed to fetch PK for {table_name}: {e}")
            return None

    def create_table_plan(self, table_name, num_rows, size_mb):
        """단일 테이블에 대한 이관 전략(Chunk 분할 수 및 기준 컬럼) 수립"""
        plan = {
            'table_name': table_name,
            'total_rows_estimated': num_rows,
            'size_mb': size_mb,
            'split_method': 'FULL_TABLE',
            'split_column': None,
            'chunks': 1,
            'threads_assigned': 1
        }
        
        # 1. Row 수가 target_chunk_size보다 작으면 쪼개지 않음 (단일 쓰레드 처리)
        if num_rows <= self.target_chunk_size:
            return plan
            
        # 2. 크기가 크면 쪼개기 시작
        pk_column = self._get_primary_key(table_name)
        
        if pk_column:
            plan['split_method'] = 'PK_RANGE'
            plan['split_column'] = pk_column
        else:
            plan['split_method'] = 'ROWID_RANGE'
            plan['split_column'] = 'ROWID'
            
        # 3. 청크 개수 및 할당할 쓰레드 계산
        num_chunks = math.ceil(num_rows / self.target_chunk_size)
        plan['chunks'] = num_chunks
        
        # 쓰레드 수는 청크 수와 max_threads 중 작은 값으로 설정
        plan['threads_assigned'] = min(num_chunks, self.max_threads)
        
        return plan

    def build_migration_plan(self, df_discovery):
        """2단계 Discovery 결과를 바탕으로 전체 이관 마스터 플랜 생성"""
        logger.info("Building Migration Master Plan based on Discovery data...")
        
        master_plan = []
        
        for index, row in df_discovery.iterrows():
            table_name = row['TABLE_NAME']
            num_rows = int(row['NUM_ROWS']) if pd.notnull(row['NUM_ROWS']) else 0
            size_mb = float(row['SIZE_MB']) if pd.notnull(row['SIZE_MB']) else 0.0
            
            logger.info(f"Planning strategy for {table_name} ({num_rows} rows)...")
            table_plan = self.create_table_plan(table_name, num_rows, size_mb)
            master_plan.append(table_plan)
            
        logger.info(f"Master plan generated for {len(master_plan)} tables.")
        return master_plan
