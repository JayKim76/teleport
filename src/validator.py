import logging
import pandas as pd

logger = logging.getLogger('Teleport.Validator')

class DataValidator:
    def __init__(self, source_config, target_config, target_schema):
        """
        source_config: AS-IS DB 접속 정보
        target_config: TO-BE DB 접속 정보
        target_schema: 검증할 스키마 (양쪽 동일 기준)
        """
        self.source_config = source_config
        self.target_config = target_config
        self.target_schema = target_schema.upper()

    def _create_connection(self, config):
        import oracledb
        dsn = f"{config['host']}:{config['port']}/{config['service_name']}"
        return oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)

    def _get_row_count(self, config, table_name):
        """특정 테이블의 정확한 Row Count(전체 건수) 조회"""
        query = f"SELECT COUNT(*) FROM {self.target_schema}.{table_name}"
        try:
            with self._create_connection(config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    result = cur.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            logger.error(f"Failed to count rows for {table_name}: {e}")
            return -1

    def run_validation(self, table_list):
        """Source와 Target DB의 Row Count를 비교하여 정합성 검증"""
        logger.info(f"Starting Data Validation for {len(table_list)} tables...")
        
        results = []
        passed_count = 0
        failed_count = 0
        
        for table in table_list:
            logger.info(f"Validating table: {table}")
            
            src_count = self._get_row_count(self.source_config, table)
            tgt_count = self._get_row_count(self.target_config, table)
            
            is_match = (src_count == tgt_count) and (src_count != -1)
            
            if is_match:
                passed_count += 1
                status = "PASS"
            else:
                failed_count += 1
                status = "FAIL"
                logger.warning(f"Mismatch found in {table}! Source: {src_count} | Target: {tgt_count}")
                
            results.append({
                'TABLE_NAME': table,
                'SOURCE_COUNT': src_count,
                'TARGET_COUNT': tgt_count,
                'DIFF': src_count - tgt_count,
                'STATUS': status
            })
            
        df_result = pd.DataFrame(results)
        
        logger.info("================ VALIDATION SUMMARY ================")
        logger.info(f"Total Tables: {len(table_list)}")
        logger.info(f"Passed: {passed_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info("====================================================")
        
        return df_result

    def gather_statistics(self):
        """마이그레이션 완료 후 Target DB의 통계 정보 갱신(Analyze) 수행"""
        logger.info("Gathering statistics (Analyze) on Target DB for CBO optimization...")
        # 오라클 내장 패키지를 사용하여 해당 스키마의 통계정보를 일괄 수집
        query = f"""
            BEGIN
               DBMS_STATS.GATHER_SCHEMA_STATS (
                  ownname => '{self.target_schema}',
                  options => 'GATHER AUTO',
                  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                  method_opt => 'FOR ALL COLUMNS SIZE AUTO',
                  cascade => TRUE
               );
            END;
        """
        try:
            with self._create_connection(self.target_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()
            logger.info("Successfully gathered statistics on Target DB.")
            return True
        except Exception as e:
            logger.error(f"Failed to gather statistics: {e}")
            return False
