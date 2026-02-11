https://github.com/delta-io/delta/releases/tag/v4.0.0
https://github.com/delta-io/delta/releases/tag/v4.0.1

# Catalog-managed tables
Catalog-managed tables는 “Delta Lake의 트랜잭션을 파일 시스템에서 Catalog로 끌어올리려는 구조적 전환의 시작”이다
https://github.com/delta-io/delta/issues/4381

# Delta Connect (Preview): Spark Connect 아키텍처에서 Delta 사용 가능(클라이언트-서버 분리형)

### 문제점
* 클라이언트마다 Spark 환경 필요
* 버전 충돌(라이브러리, JVM, Python)
* 보안/격리 어려움
* “Spark를 직접 띄우지 않고 Delta를 쓰고 싶다”는 요구 증가

---

## 2️⃣ Spark Connect란?

**Spark Connect**는 Spark 3.4+에서 도입된 아키텍처로:

## 3️⃣ Delta Connect란?

### 정의

> **Delta Connect는
> Spark Connect 환경에서
> Delta Lake 테이블을 읽고/쓰게 해주는 Delta 확장**

즉:

* Spark Connect + Delta Lake를
* **공식적으로 호환**시킨 것

### ① Delta를 “라이브러리”가 아니라 “서비스”로 사용
👉 **완전한 중앙 통제 모델**

> **Delta Connect는
> “Delta Lake를 클라이언트 라이브러리에서
> 서버-사이드 데이터 서비스로 바꾸는 첫 단계”다**

# Variant data type 지원
* `STRING`이 아님
* `STRUCT`가 아님
* **“타입을 가진 JSON”**

## 4️⃣ Variant는 단순 JSON이 아니다 (중요)

### 기존 JSON STRING과의 차이

| 항목       | JSON STRING | Variant |
| -------- | ----------- | ------- |
| 타입 정보    | ❌           | ✅       |
| 엔진 간 일관성 | 낮음          | 높음      |
| 필드 접근    | 파싱 필요       | 네이티브    |
| 쿼리 최적화   | 어려움         | 가능      |
| 오류       | 런타임 파싱 오류   | 타입 안전성  |

---

## 7️⃣ 왜 이게 중요한가? (실무 관점)

### ① Bronze 레이어에 최적

* 원본 이벤트
* 장비 로그
* Trace / payload
* vendor-specific metadata

👉 “다 뜯어 고치지 말고 그대로 받아라”

---

### ② 스키마 진화 비용 최소화

* 신규 필드 추가
* 일부 레코드만 구조 변경
* 이전 데이터 영향 없음

---

### ③ Gold/Silver로 점진적 정규화

* Bronze: Variant
* Silver: 필요한 필드만 추출
* Gold: 완전한 정형 스키마

---

## 8️⃣ Shredded Variant와의 관계 (미리보기)

Variant는:

* 유연하지만
* 전체 스캔 시 느릴 수 있음

→ 그래서 나온 게:

* **Shredded Variants (Preview)**

  * 자주 쓰는 필드를 물리적으로 분리 저장

즉:

> Variant = 유연성
> Shredding = 성능 최적화