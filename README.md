# 프로젝트 설명

SimpleDb는 JDBC를 활용하여 MySql과 통신할 수 있도록 하는 라이브러리입니다. 간단한 CRUD 기능을 제공하며, 트랜잭션, 멀티 스레드 환경에서 안정적인 DB 접근을 지원합니다.

# 데이터 통신 흐름

![image](https://github.com/user-attachments/assets/e30838d3-bab0-4c9d-baa7-46e78ec84ee7)


# 주요 기능

- SQL 쿼리 결과를 Map, List, POJO로 변환
- 트랜잭션(commit, rollback) 지원
- 멀티 스레드 환경에서 ThreadLocal을 사용한 안정적인 커넥션 관리

# 테스트

TDD로 개발되어 다양한 쿼리, 트랜잭션, 멀티 스레딩 등 다양한 테스트 케이스가 포함되어 있으며, 테스트 코드를 통해 실제 사용 예시와 동작을 확인할 수 있습니다.
