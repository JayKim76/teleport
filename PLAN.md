# Project Teleport: Oracle Migration Solution

## 1. 프로젝트 개요 (Overview)
- **프로젝트명**: 텔레포트 (Teleport)
- **목적**: 오라클(Oracle) 데이터베이스를 이기종 데이터베이스(PostgreSQL, MySQL, Cloud Native DB 등)로 안전하고 빠르게 마이그레이션(이관)하는 자동화 솔루션 개발.

## 2. 주요 기능 요구사항 (예상)
1. **스키마 변환 (Schema Conversion)**: Oracle 특화 객체(PL/SQL, Packages, Sequences 등)를 타겟 DB에 맞게 변환.
2. **초기 데이터 적재 (Initial Data Load)**: 대용량 데이터의 고속 병렬 추출 및 적재.
3. **변경 데이터 캡처 (CDC - Change Data Capture)**: 무중단 마이그레이션을 위한 실시간 데이터 동기화.
4. **정합성 검증 (Data Validation)**: 이관 전/후 데이터 개수 및 해시 검증.

## 3. 질문 사항 (To Boss)
- **Source DB**: Oracle (버전 범위는?)
- **Target DB**: 어떤 DB로 주로 마이그레이션 할 예정이신가요? (예: PostgreSQL, MySQL, AWS Aurora 등)
- **핵심 언어/프레임워크**: 솔루션 개발에 사용할 주력 언어는 무엇으로 할까요? (예: Python, Java, Go, C++ 등)
