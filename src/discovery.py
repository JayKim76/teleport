import logging
import pandas as pd

logger = logging.getLogger('Teleport.Discovery')

class OracleDiscoverer:
    def __init__(self, db_connector, target_schema):
        """
        db_connector: DBConnector 인스턴스 (Oracle)
        target_schema: 분석할 오라클 유저/스키마 이름
        """
        self.db_connector = db_connector
        self.target_schema = target_schema.upper()

    def _fetch_as_dataframe(self, query):
        """오라클 쿼리 결과를 Pandas DataFrame으로 반환"""
        try:
            with self.db_connector.get_oracle_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    columns = [col[0] for col in cur.description]
                    data = cur.fetchall()
                    return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return None

    def run_full_discovery(self):
        """환경 분석을 수행하고 전체 테이블 메타데이터를 DataFrame으로 반환"""
        logger.info(f"Starting Assessment & Discovery for schema: {self.target_schema}...")
        
        # 1. 테이블 기본 정보 (Row count 및 통계 수집일)
        q_tables = f"""
            SELECT TABLE_NAME, NUM_ROWS, LAST_ANALYZED
            FROM ALL_TABLES
            WHERE OWNER = '{self.target_schema}'
        """
        
        # 2. 테이블 물리적 용량 정보 (MB)
        q_sizes = f"""
            SELECT SEGMENT_NAME AS TABLE_NAME, 
                   ROUND(SUM(BYTES)/1024/1024, 2) AS SIZE_MB
            FROM ALL_SEGMENTS
            WHERE OWNER = '{self.target_schema}' AND SEGMENT_TYPE LIKE 'TABLE%'
            GROUP BY SEGMENT_NAME
        """
        
        # 3. 고급 기능(LOB, 파티션) 사용 여부 확인
        # LOB 컬럼이 있거나 파티션된 테이블은 마이그레이션 전략(Chunking)을 다르게 가져가야 함.
        q_features = f"""
            SELECT t.TABLE_NAME, 
                   NVL(p.PARTITIONING_TYPE, 'NONE') AS PARTITIONED,
                   CASE WHEN l.TABLE_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS HAS_LOB
            FROM ALL_TABLES t
            LEFT JOIN ALL_PART_TABLES p ON t.OWNER = p.OWNER AND t.TABLE_NAME = p.TABLE_NAME
            LEFT JOIN (SELECT DISTINCT OWNER, TABLE_NAME FROM ALL_LOBS WHERE OWNER = '{self.target_schema}') l 
              ON t.OWNER = l.OWNER AND t.TABLE_NAME = l.TABLE_NAME
            WHERE t.OWNER = '{self.target_schema}'
        """
        
        df_tables = self._fetch_as_dataframe(q_tables)
        df_sizes = self._fetch_as_dataframe(q_sizes)
        df_features = self._fetch_as_dataframe(q_features)
        
        if df_tables is not None and df_sizes is not None and df_features is not None:
            # 테이블명 기준으로 DataFrame 병합
            final_df = df_tables.merge(df_sizes, on='TABLE_NAME', how='left')
            final_df = final_df.merge(df_features, on='TABLE_NAME', how='left')
            
            # 결측치(NULL) 0으로 처리 및 용량순 정렬
            final_df.fillna({'SIZE_MB': 0, 'NUM_ROWS': 0}, inplace=True)
            final_df = final_df.sort_values(by='SIZE_MB', ascending=False).reset_index(drop=True)
            
            logger.info(f"Discovery completed successfully. Found {len(final_df)} tables.")
            return final_df
        else:
            logger.error("Discovery failed. Check database connection and permissions.")
            return None
