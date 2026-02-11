# Type Widening (Preview)

**Type Widening은 “대용량 테이블에서 정수 타입을 더 크게 바꾸는 일을, 파일 재작성 없이 즉시·저비용으로 처리하게 해주는 스키마 진화 기능”**이며, Preview 단계에서는 필요 시 명시적 리라이트로 호환성을 되돌릴 수 있게 설계되었습니다.

## 목적 (Why)

1. **스키마 진화 비용 최소화**

   * 데이터 증가로 기존 정수 타입이 **오버플로우 위험**에 도달할 때(예: `byte` 범위 초과),
     **테이블 전체를 다시 쓰지 않고** 더 큰 타입으로 확장하려는 목적입니다.

2. **운영 중단 없이 안전한 타입 확장**

   * 대용량 테이블에서 타입 변경은 재작성 비용·시간·리스크가 큼 →
     **메타데이터 레벨 변경**으로 즉시 대응 가능하게 함.
   * `byte → short → int` 같은 **확장(widening)**을 **파일 재작성 없이** 수행 →
     수백 TB급 테이블에서도 **즉시 적용**.
   * `OPTIMIZE`/full rewrite 불필요 → **IO·컴퓨트 비용 절감**, 작업 시간 단축.

## 제약과 주의점 (중요)

* **확장만 가능**: 축소(narrowing, 예: `int → short`)는 불가.
* **Preview 제약**: 초기에는 **정수 계열 중심**으로 제한적 지원.
* **엔진 호환성**: Type Widening을 이해하지 못하는 엔진에서는
  **읽기/쓰기 제약**이 생길 수 있음 → 필요 시 리라이트 선택.

## 사용법

### 0) 사전 체크 (필수)

* **Delta Lake 3.2 이상**에서 Type Widening(Preview)을 사용할 수 있습니다. ([Delta Lake][1])
* Type Widening이 켜진 테이블은 **3.2+에서만 읽기/쓰기가 보장**됩니다(즉, 더 낮은 버전/클라이언트 호환성 이슈 가능). ([Delta Lake][1])

---

### 1) 테이블에 Type Widening(Preview) 활성화

기존 테이블에 켜는 가장 표준 방법은 **테이블 프로퍼티**입니다. ([Delta Lake][1])

```sql
ALTER TABLE <db>.<table>
SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
```

> 이걸 하면 Delta Lake 3.2에서는 내부적으로 **table feature `typeWidening-preview`**가 활성화되는 방식으로 동작합니다. ([Delta Lake][1])

---

### 2) 컬럼 타입을 “더 큰 타입”으로 변경

이제 widen 가능한 타입 변경을 수행합니다(예: `BYTE -> INT`).
(Delta 4.0 관련 글에서는 `ALTER TABLE … CHANGE COLUMN … TYPE …`를 예로 듭니다. ([delta.io][2]))

```sql
ALTER TABLE <db>.<table>
CHANGE COLUMN my_col my_col INT;
```

* 여기서 핵심은 **“파일을 재작성하지 않고” 메타데이터/로그 레벨에서 타입 확장을 기록**한다는 점입니다. ([Delta Lake][1])

> 주의: “widening”만 됩니다. (예: `INT -> BYTE` 같은 narrowing은 컨셉상 해당 기능 범위 밖)

### Trino를 같이 쓰는 경우(현실 팁)

* Type Widening은 “테이블 feature”로 기록되는 변화라서, **읽는 쪽 엔진이 이 feature를 얼마나 이해하느냐**가 중요합니다.
* Trino 쪽에서는 “이런 테이블을 읽는 테스트를 추가”하는 이슈가 있었고, “connector에서 SET DATA TYPE 지원은 범위 밖”이라는 언급도 있습니다. 즉 **쓰기/스키마 변경은 Spark(Delta)에서 하고, Trino는 읽기 호환성만 확인**하는 패턴이 안전합니다. ([GitHub][4])