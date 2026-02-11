# 1) Liquid Clustering이 해결하려는 문제

Delta 테이블 성능은 본질적으로 다음 2가지에 의해 좌우됩니다.

1. **스캔해야 하는 파일 수** (Small file problem 포함)
2. **조건절(predicate)에 의해 “읽지 않아도 되는 파일”을 얼마나 잘 건너뛰는지(= data skipping / file pruning)**

Delta는 각 데이터 파일(보통 Parquet)의 **파일 단위 통계(min/max/nullCount/rowCount 등)** 를 메타데이터에 저장해두고, 쿼리 조건절로 “이 파일은 절대 결과가 나올 수 없다”를 판정하면 그 파일 자체를 스캔에서 제외합니다. 이게 *data skipping*의 핵심입니다. ([Delta Lake][1])

그런데, 파일 안에 값이 “뒤섞여” 있으면(예: 같은 eqp_id가 파일 전체에 산재) 파일의 min/max 범위가 너무 넓어서 “건너뛸 파일”이 잘 안 생깁니다. 그래서 **“파일마다 특정 컬럼 값의 범위를 좁게 만들도록 데이터를 재배치”** 하는 기법이 필요합니다.

---

# 2) 전통적 방식: Partitioning과 Z-Order의 한계

## 2.1 Hive-style Partitioning

파티셔닝은 “디렉터리(폴더) 단위로 파일을 분리”합니다. 예: `date=2026-02-11/eqp_id=E01/...`
조건절에 partition 컬럼이 있으면 폴더 자체를 통째로 제외할 수 있어 강력합니다.

하지만 다음 문제가 있습니다(특히 고카디널리티/스큐/변화하는 데이터에 취약):

* 파티션이 너무 많아져 메타데이터/파일 수 폭증
* 스큐가 심하면 어떤 파티션은 파일이 너무 크고, 어떤 파티션은 너무 작아짐
* 파티션 전략을 바꾸려면 **테이블을 사실상 재작성(대규모 rewrite)** 해야 함 ([Delta][2])

## 2.2 Z-Order(OPTIMIZE ZORDER BY)

Z-Order는 “다차원 값을 1차원으로 매핑해서 정렬한 뒤 파일로 쓰기”라는 점에서 Liquid와 비슷한 계열입니다. 하지만 Delta가 Liquid Clustering을 도입한 배경에는 Z-Order의 구조적 한계가 큽니다:

* `OPTIMIZE ZORDER BY`는 (해당 범위에 대해) **늘 크게 다시 리클러스터링(rewrite)** 하는 성격이라 **write amplification**이 큼
* 클러스터링 컬럼을 테이블에 “기억”시키지 못해 사용자가 매번 `ZORDER BY(...)`를 반복 지정해야 하고 실수 유발 ([Delta][2])

Delta 공식 문서도 `OPTIMIZE ZORDER BY`는 “(특정 케이스에서) 반복 수행 시 효율이 떨어질 수 있고, compaction처럼 항상 안전한 ‘멱등(idempotent)’ 작업으로 보기 어렵다”고 명시합니다. ([Delta Lake][1])

---

# 3) Liquid Clustering의 정의 (한 줄)

**Liquid Clustering = “클러스터링 컬럼을 테이블 메타데이터에 저장해두고, OPTIMIZE가 *증분(incremental)* 으로 필요한 파일만 골라 Hilbert Curve 기반으로 재배치하는 방식”** 입니다. ([Delta][2])

핵심 키워드 3개만 먼저 잡으면 이해가 빨라집니다.

1. **Clustering columns가 테이블에 저장됨** (매번 지정할 필요 없음) ([Delta][2])
2. **Hilbert Curve** 기반 다차원 클러스터링 (Z-Order보다 data skipping 개선 목표) ([Delta][2])
3. **ZCube**라는 “증분 클러스터링 단위”를 도입해서 **이미 잘 정리된 파일은 다시 안 건드리는** 쪽으로 설계 ([Delta][2])

---

# 4) “파일과 데이터가 어떤 구조로 배치되는가” — 물리 구조부터

## 4.1 Delta 테이블의 디렉터리 구조(큰 그림)

일반적인 Delta 테이블(파티션 없다고 가정)은 대략 이렇게 생깁니다.

```
/table_path/
  part-00000-....snappy.parquet
  part-00001-....snappy.parquet
  ...
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
    ...
    00000000000000000010.checkpoint.parquet
    ...
```

* 루트에는 **데이터 파일(대개 Parquet)** 들이 있고
* `_delta_log/`에 **트랜잭션 로그**가 있어 “현재 스냅샷에서 읽어야 할 파일 목록”이 결정됩니다.

Liquid Clustering은 **데이터 파일의 “배치(어떤 값들이 어떤 파일에 몰리느냐)”를 OPTIMIZE로 바꿔치기** 하는 기능이고, 그 사실/단위 정보가 **Delta 로그 메타데이터에 기록**됩니다. ([Delta][2])

## 4.2 data skipping 통계(“파일마다 min/max”)

Delta는 파일 단위로 컬럼 통계를 수집해 data skipping에 사용합니다. 기본적으로 “앞쪽 N개 컬럼”만 통계를 수집하고(N 기본값이 32), 테이블 속성으로 조정할 수 있습니다. ([Delta Lake][1])

* 기본: `delta.dataSkippingNumIndexedCols = 32`
* 모든 컬럼: `delta.dataSkippingNumIndexedCols = -1` ([Delta Lake][3])

**중요:** Liquid Clustering에서 선택한 클러스터링 컬럼은 “통계가 수집되는 컬럼”이어야 의미가 있습니다. (통계가 없으면 스킵을 못 하니까요.) ([Delta][4])

---

# 5) Liquid Clustering의 작동 원리 (Hilbert + ZCube)

## 5.1 Hilbert Curve가 뭐고, 왜 쓰나?

Liquid는 다차원(최대 4개 컬럼) 값을 **Hilbert curve라는 space-filling curve**로 1차원 값(정렬키)로 매핑합니다. 그 1차원 값으로 정렬하면 “다차원 공간에서 가까운 점들이 디스크에서도 가깝게” 배치되는 성질이 생깁니다. ([Delta][2])

> 직관: `(A,B,C)` 3차원 좌표를 “한 줄로 쭉 펴서” 번호를 매기는데, 그 번호가 “공간상의 이웃 관계”를 꽤 잘 보존하도록 만드는 방식.

그 다음 OPTIMIZE는 이 정렬 순서대로 레코드를 다시 써서 **각 파일이 클러스터링 컬럼 기준으로 ‘좁은 범위’를 가지도록** 만듭니다. 그러면 조건절이 들어왔을 때 “이 파일은 범위 밖이므로 스캔 제외”가 크게 늘어납니다. ([Delta][2])

## 5.2 ZCube: Liquid Clustering을 ‘증분’으로 만드는 핵심 개념

Delta Lake 3.1에서 Liquid를 소개할 때, **증분 클러스터링의 단위를 “ZCube”** 라고 부릅니다. 공식 설명은 이렇습니다:

* **ZCube = 동일한 OPTIMIZE 실행으로 생성된, Hilbert-clustered 데이터 파일들의 “그룹”**
* 이미 클러스터링된 파일은 **Delta 로그(파일 메타데이터)에 ZCube id로 태그**되고,
* 이후 OPTIMIZE는 **태그가 없는(= 아직 클러스터링 안 된) 파일만 주로 다시 작성**해서 write amplification을 크게 줄입니다. ([Delta][2])

여기서 “Cube”라는 이름 때문에 **“3D/4D 공간을 물리적으로 ‘정육면체’로 잘라 저장하나?”** 라고 오해하기 쉬운데, 정확히는:

* *ZCube는 물리적인 폴더/파티션이 아닙니다.*
* Delta 로그에서 “이 파일들은 **같은 클러스터링 작업의 산출물**이며, **같은 클러스터링 전략으로 정렬된 묶음**이다”를 추적하기 위한 **논리적 배치 단위**에 가깝습니다. ([Delta][2])

### “파일 배치” 관점에서 ZCube를 어떻게 상상하면 좋나?

아래처럼 생각하면 정확도가 높습니다.

* 클러스터링 컬럼이 `(c1, c2, c3)`라면
* 각 레코드는 Hilbert index `h(c1,c2,c3)`를 갖게 되고
* OPTIMIZE는 `h`로 정렬한 다음 “타겟 파일 크기”에 맞춰 연속 구간을 잘라 파일로 씁니다.
* 이때 **연속 구간(= 비슷한 h 범위)** 로 만들어진 파일 묶음이 곧 **한 ZCube**가 됩니다. ([Delta][2])

즉, “ZCube 안의 파일들”은 **서로 비슷한 다차원 값들을 포함**하도록 만들어지고, 그 상태를 “이 ZCube에 속한 파일”로 로그에 표시해두는 것입니다.

---

# 6) OPTIMIZE가 Liquid Clustering에서 하는 일 (3.3.x 기준)

## 6.1 OPTIMIZE의 기본 성격: compaction + (필요 시) 클러스터링

Delta의 OPTIMIZE는 기본적으로 **작은 파일들을 큰 파일로 합치는(compaction)** 용도이며, 이는 small file problem을 줄이는 대표적 방법입니다. ([Delta Lake][1])

* 기본 compaction은 “작은 파일 → 큰 파일”로 재작성 (bin-packing) ([Delta Lake][1])
* 일반적으로 OPTIMIZE는 **동시 읽기/쓰기와 공존**할 수 있게 트랜잭션 로그 기반으로 동작합니다(커밋 시점에 원자적으로 파일 교체). ([Delta Lake][1])

Liquid Clustering 테이블에서는, OPTIMIZE가 compaction을 하면서 **클러스터링 컬럼 기준으로(테이블에 저장된 정보 기반으로) Hilbert 정렬을 적용**하는 방향으로 작동합니다. ([Delta][2])

## 6.2 “증분”이라는 말의 정확한 의미

Delta docs는 Liquid clustering을 이렇게 규정합니다:

* Liquid clustering은 **incremental**이다.
* “데이터가 계속 들어오는 동안에도” 최적화를 수행할 수 있다.
* **다른 클러스터링 컬럼으로 이미 클러스터링된 파일은 자동으로 다시 쓰지 않는다** (즉, 키를 바꾸면 과거 데이터는 그대로 남을 수 있음). ([Delta Lake][5])

여기서 ZCube 태그가 중요한 역할을 합니다. “이미 Hilbert-clustered 결과물”을 표시해두면 다음 OPTIMIZE가 “어디부터 다시 손대야 하는지”를 빠르게 결정할 수 있으니까요. ([Delta][2])

---

# 7) OPTIMIZE FULL이 왜 중요한가 (3.3의 핵심 변화)

Delta Lake 3.3 릴리즈 하이라이트에 Liquid clustering 관련으로 딱 3개가 들어갑니다:

* **OPTIMIZE FULL**
* 기존 **unpartitioned 테이블**에 clustering enable
* **external location**에서 clustered table 생성 ([Delta][6])

즉, 3.3에서 Liquid Clustering을 “운영 가능한 기능”으로 만드는 실질적 완성도가 올라갔다고 보면 됩니다.

## 7.1 OPTIMIZE FULL의 의미: “전체 리클러스터링”

Liquid clustering의 일반 OPTIMIZE는 기본적으로 “필요한 것만” 증분으로 고칩니다.
그런데 아래 상황에서는 “전체를 다시 정렬”하고 싶은 니즈가 생깁니다.

* Liquid Clustering을 **처음 도입했는데**, 테이블에 이미 과거 데이터 파일이 잔뜩 존재
* `CLUSTER BY` 컬럼을 **변경**했는데, 기존 파일들은 옛 키 기준(또는 무질서)로 남아 있음
* 과거에 데이터가 크게 어지럽혀졌고(예: 대규모 MERGE/UPDATE/streaming small files), “전체를 다시 잡고” 싶음

이걸 위해 3.3에서 들어온 게 `OPTIMIZE ... FULL` 입니다. 문서상 예시는 다음과 같습니다. ([Delta Lake][5])

```sql
OPTIMIZE table_name FULL;
```

**직관적으로는** “증분 최적화”가 아니라 **테이블 전체 파일을 대상으로 Hilbert 클러스터링 재배치(= 전면 리빌드)** 를 수행한다고 보면 됩니다. ([Delta Lake][5])

## 7.2 FULL을 해야만 ‘완전히’ 좋아지는 대표 케이스

Liquid clustering의 문서가 말하는 중요한 포인트가 이것입니다:

> “클러스터링 컬럼이 다른 상태로 이미 클러스터링된 파일은 자동으로 다시 쓰지 않는다.” ([Delta Lake][5])

즉, `ALTER TABLE ... CLUSTER BY (new_cols...)`로 키를 바꿨을 때:

* **새로 들어오는 데이터 + 새 OPTIMIZE 결과물**은 *new_cols* 기준으로 ZCube가 만들어지지만
* **기존 데이터 파일들은 old_cols(또는 무정렬)** 상태로 남아 “혼재”할 수 있습니다.

이 혼재 상태는 “시간이 지나면 자연히 치유”되기도 하지만(새 데이터가 많고 자주 OPTIMIZE하면 점진적으로 new layout 비중 증가),
**깔끔하게 한 번에 정리하려면 FULL이 필요**합니다. ([Delta Lake][5])

---

# 8) Liquid Clustering을 쓰는 테이블의 제약/요건 (3.3.x)

## 8.1 테이블 프로토콜/기능 플래그

Delta 문서는 Liquid clustering 테이블이 다음을 요구한다고 명시합니다:

* **Table features: `Clustering` + `DomainMetadata`**
* 최소 **Reader Version 1, Writer Version 7**
* 이 기능들을 지원하지 않는 writer는 테이블에 쓰기 불가(프로토콜 상) ([Delta Lake][5])

이건 운영에서 중요합니다. “클러스터링을 켠 순간” 모든 writer/엔진이 그 테이블 기능을 이해해야 합니다.

## 8.2 컬럼 제한: 최대 4개, 그리고 “통계 수집되는 컬럼”이어야 함

Liquid clustering 블로그/문서가 직접 못 박는 제약은:

* 클러스터링 컬럼은 **최대 4개**
* 클러스터링 컬럼은 **Delta 로그에 통계가 수집되는 컬럼**이어야 함
* 기본 통계 수집은 “처음 32 컬럼”이며 `delta.dataSkippingNumIndexedCols`로 조정 가능 ([Delta][4])

> 실무 팁: “쿼리에서 가장 자주 필터/조인에 쓰는 컬럼”이 **스키마 앞쪽 32개 밖**에 있으면, Liquid를 켰는데도 data skipping이 기대만큼 안 나올 수 있습니다. 이때는 (1) 스키마 컬럼 순서 조정, (2) `delta.dataSkippingNumIndexedCols` 상향/`-1`, 같은 접근을 검토합니다. ([Delta Lake][1])

---

# 9) (매우 실무적인) “내 테이블에서 파일이 어떻게 바뀌는가” 시나리오

당신이 Liquid Clustering을 이미 쓰고 있다고 했으니, 체감이 가장 큰 대표 흐름을 하나로 묶어 설명해볼게요.

## 9.1 초기 상태: 스트리밍/빈번한 배치로 small files + 값 뒤섞임

* ingestion이 계속 append → 작은 파일이 많이 생김
* 각 파일에는 eqp/param/time 값이 뒤섞여 있음
* 결과: 파일 min/max 범위가 넓어 data skipping 약함 ([Delta Lake][1])

## 9.2 OPTIMIZE(일반) 실행: “새/어지러운 구간만” 재정렬 + ZCube 생성

OPTIMIZE는 대략 이런 변화를 만들었다고 이해하면 됩니다.

* (A) 작은 파일들을 읽어들여
* (B) 클러스터링 컬럼 기반 Hilbert index를 계산한 뒤 정렬하고
* (C) 타겟 파일 크기에 맞춰 큰 파일로 다시 씀(= compaction)
* (D) 커밋 시점에 “옛 파일 제거 + 새 파일 추가”
* (E) 그리고 이 새 파일 묶음을 “ZCube id”로 로그에 태깅 ([Delta][2])

이후 쿼리가 `(eqp_id = 'E01' AND created_time between ... )` 같은 조건을 쓰면,
파일 단위 min/max가 더 촘촘해져 “읽을 파일 수”가 줄 가능성이 커집니다. ([Delta Lake][1])

## 9.3 OPTIMIZE FULL 실행: “테이블 전체를 한 번에 새 레이아웃으로”

FULL은 위 작업을 “부분”이 아니라 “전체”에 대해 수행해서,

* 과거 데이터까지 포함해 한 번에 “현재 CLUSTER BY 키” 기준으로 정렬 상태를 맞추는 효과가 있습니다. ([Delta Lake][5])

---

# 10) 4.x에서는 뭐가 달라졌나? (Liquid 관점 중심)

Delta Lake 4.0은 큰 릴리즈이며, (프리뷰 시점 기준) Apache Spark 4.0 프리뷰 기반에서 **Delta Connect, Coordinated Commits, Variant, Type Widening** 등 폭이 넓은 기능들을 강조합니다. ([Delta][7])
또한 4.x 라인업은 4.0/4.1 등으로 릴리즈가 이어졌습니다. ([Delta][8])

다만 **Liquid Clustering 자체의 “핵심 개념(= Hilbert + ZCube + 증분 OPTIMIZE, 그리고 FULL로 전체 재정렬)”은 3.3에서 이미 ‘운영 완성형’으로 크게 정리된 뒤** 4.x 릴리즈 포스트에서는 Liquid만을 크게 바꾸는 내용이 전면에 나오진 않습니다. (즉, 4.x에서 *Liquid의 철학이 바뀌었다*고 말할 근거는 공개 릴리즈 하이라이트만으로는 제한적입니다.) ([Delta][6])

---

# 11) 체크리스트: “나는 Liquid를 쓰는데 개념을 몰랐다”에 딱 맞는 점검 포인트

1. **CLUSTER BY 컬럼이 쿼리 predicate에 진짜 자주 등장하는가?**
2. 그 컬럼들이 **통계 수집 대상**인가? (`delta.dataSkippingNumIndexedCols`) ([Delta Lake][3])
3. 테이블에 과거 데이터가 많다면, “일반 OPTIMIZE만으로 충분했나, FULL을 한 번 해야 하나?” ([Delta Lake][5])
4. 쓰기 엔진이 여러 개라면(예: Trino + Spark 혼용), **클러스터링 테이블 기능(Clustering/DomainMetadata)을 writer들이 모두 이해하는가?** ([Delta Lake][5])

---

## 다음 단계(원하면)

원하시면, 당신의 실제 워크로드를 기준으로:

* (A) **CLUSTER BY 후보 컬럼 조합(최대 4개)** 을 어떻게 고르는 게 합리적인지
* (B) “언제 OPTIMIZE, 언제 OPTIMIZE FULL”을 돌릴지(빈도/조건)
* (C) `delta.dataSkippingNumIndexedCols`를 올릴 때의 비용/부작용(로그 크기/통계 수집 비용)
  을 **현재 테이블 스키마/쿼리 패턴** 기준으로 더 구체화해서 “운영 가이드” 형태로 정리해드릴게요.

[1]: https://docs.delta.io/optimizations-oss/ "Optimizations | Delta Lake"
[2]: https://delta.io/blog/delta-lake-3-1/ "Delta Lake 3.1.0 | Delta Lake"
[3]: https://docs.delta.io/table-properties/?utm_source=chatgpt.com "Delta Table Properties Reference"
[4]: https://delta.io/blog/liquid-clustering/?utm_source=chatgpt.com "Delta Lake Liquid Clustering"
[5]: https://docs.delta.io/delta-clustering/ "Use liquid clustering for Delta tables | Delta Lake"
[6]: https://delta.io/blog/delta-lake-3-3/ "Delta Lake 3.3 | Delta Lake"
[7]: https://delta.io/blog/delta-lake-4-0/ "Delta Lake 4.0 Preview | Delta Lake"
[8]: https://delta.io/blog/2025-09-25-delta-lake-40/ "Delta Lake 4.0 | Delta Lake"

---


































아래 내용은 **Delta Lake 3.3.x 기준**으로, *Liquid Clustering*과 특히 **`OPTIMIZE FULL`**이 “정확히 무엇을 더 하는지”, “어떤 메타데이터/구조를 남기는지”, “ZCube가 왜 등장하는지”를 **구조(트랜잭션 로그/파일 배치) 중심으로** 최대한 쉽게 풀어쓴 “보고서형” 설명입니다. (중간에 확인용으로 직접 `_delta_log`에서 볼 수 있는 흔적도 같이 넣었습니다.)

---

## 1) Liquid Clustering이 해결하려는 문제

### 1.1 전통적 방식(Partition / ZORDER)의 한계

* **Hive-style partition**: 디렉터리 레벨로 데이터를 나눠서 *partition pruning*을 극대화하지만

  * 파티션 키를 잘못 잡으면 *과다 파티션(파일 폭증)* 또는 *과소 파티션(스캔 과다)*가 발생.
  * 쿼리 패턴이 바뀌면 파티션 설계를 다시 하기가 매우 비쌈(대규모 rewrite/재적재).
* **Z-Order (ZORDER BY)**: 파티션 내부(또는 비파티션 테이블)에서 다차원 locality를 개선하지만

  * “그때그때 지정한 컬럼” 기준이라 운영 관점에서 **지속적/점진적 유지**가 어렵고,
  * “전체를 다시 정렬”하는 느낌이라 비용이 커질 수 있음.

### 1.2 Liquid Clustering의 핵심 아이디어

Liquid Clustering은 **“파티션처럼 경직된 디렉터리 분할” 대신**, 테이블 내부 파일들을 **클러스터링 키(컬럼) 기준으로 점진적으로 재배치**해서,

* 쿼리에서 클러스터링 컬럼으로 필터할 때 **파일 통계(min/max) 기반 data skipping**이 잘 먹도록 만들고,
* 데이터/쿼리 패턴 변화에 맞춰 **키를 바꿔도(ALTER CLUSTER BY)** “새로 들어오는 데이터부터” 반영 가능하게 하며,
* 필요하면 **`OPTIMIZE FULL`로 기존 데이터까지 한 번에 재클러스터링**합니다. ([Delta Lake][1])

---

## 2) Liquid Clustering을 “프로토콜/로그 관점”에서 보면

Liquid Clustering은 “그냥 OPTIMIZE가 더 똑똑해진 것”이 아니라, **Delta 로그에 ‘이 테이블은 클러스터드 테이블이다’라는 표준화된 흔적을 남기도록** 설계돼 있습니다.

### 2.1 Delta 프로토콜이 요구하는 2가지 흔적

Delta 프로토콜(로그 스펙)에서 **클러스터드 테이블(Clustered Table)**은 Writer가 다음을 써야 한다고 명시합니다.

1. **도메인 메타데이터(domainMetadata)에 클러스터링 컬럼을 기록**

   * domain: `delta.clustering`
2. 파일을 추가(add)할 때 **`clusteringProvider` 필드를 채워야 함** (이 파일이 어떤 클러스터링 구현으로 쓰였는지) ([GitHub][2])

즉, Liquid Clustering을 켠 테이블은 `_delta_log`에 “클러스터링 키/공급자”가 **구조적으로 박힙니다.**

---

## 3) ZCube는 뭐고, 왜 나오나?

### 3.1 ZCube를 한 문장으로

**ZCube = “클러스터링(재정렬) 작업 단위로 생성된 파일 묶음(논리적 배치/세대)”**라고 이해하면 가장 안전합니다.

Liquid Clustering은 “매번 전체를 갈아엎는” 대신, 보통은 **‘필요한 파일만’** 뽑아서 재작성(rewrite)하는데, 이때 **어떤 파일들이 같은 클러스터링 작업으로 생성됐는지**를 식별하고, 이후의 점진적 유지/재클러스터링 판단에 활용하기 위해 **ZCube ID 같은 태그를 남깁니다.**

### 3.2 ZCube 흔적(태그)은 어디에 남나?

일반적으로 “클러스터링된 파일”은 Delta 로그의 `add` 액션에 **tags**로 ZCube 관련 값을 남깁니다. 예: `ZCUBE_ID`, `ZCUBE_ZORDER_BY`, `ZCUBE_ZORDER_CURVE` 등

> 주의: 위 태그 이름/형태는 구현/버전에 따라 조금씩 달라질 수 있지만, 핵심은 **“이 파일은 (어떤 키/어떤 커브/어떤 큐브 작업) 결과물이다”**를 로그에 남긴다는 점입니다.

---

## 4) `OPTIMIZE`가 Liquid Clustering에서 하는 일

### 4.1 Liquid Clustering이 없는 테이블에서의 OPTIMIZE

Delta 문서의 일반 OPTIMIZE는 기본적으로 “작은 파일을 큰 파일로 합치는(compaction/bin-packing)” 성격이 큽니다. ([Delta Lake][3])

### 4.2 Liquid Clustering이 있는 테이블에서의 OPTIMIZE

Databricks 문서에서도 요약이 잘 되어 있는데,

* Liquid Clustering이 켜져 있으면 `OPTIMIZE`는 **클러스터링 키 기준으로 파일을 재작성**하여 데이터 레이아웃을 개선합니다.
* 테이블이 파티션을 가지고 있으면, **파티션 내부에서** 이런 최적화가 수행됩니다. ([Databricks Docs][4])

여기서 중요한 포인트는:

> Liquid Clustering의 기본 `OPTIMIZE`는 보통 “점진적(incremental)”로 동작하며, **이미 충분히 잘 클러스터링된 영역/파일은 매번 전체 재작성하지 않는 방향**으로 설계된다는 점입니다.
> (이 “점진성”을 구현하는 핵심 장치 중 하나가 **ZCube 계열 메타데이터**라고 보면 됩니다.)

---

## 5) Delta Lake 3.3의 `OPTIMIZE FULL`은 무엇이 다른가?

### 5.1 공식 정의(3.3+)

Delta Lake 문서(3.3+)는 `OPTIMIZE FULL`을 다음처럼 정의합니다.

* **전체 테이블의 모든 레코드에 대해 강제로 reclustering 수행**
* 특히 **클러스터링 컬럼을 바꾼 경우**, 기존 데이터는 자동으로 다시 쓰이지 않기 때문에 `OPTIMIZE FULL`을 권장
* 이미 `OPTIMIZE FULL`을 수행했고 클러스터링 컬럼 변화가 없다면, `OPTIMIZE FULL`은 일반 `OPTIMIZE`와 동일하게 동작 ([Delta Lake][1])

즉:

* `OPTIMIZE`(기본): “대개 필요한 부분만 점진적으로”
* `OPTIMIZE FULL`: “**(필요 시) 전체 데이터까지 포함해서** 현재 클러스터링 키에 맞도록 강제 재배치”

### 5.2 왜 `OPTIMIZE FULL`이 필요한가?

문서에 있는 핵심 문장이 이겁니다.

* `ALTER TABLE ... CLUSTER BY (...)`로 **클러스터링 컬럼을 바꾸면**

  * 이후에 들어오는 데이터와 이후 OPTIMIZE는 새 키를 쓰지만
  * **기존 데이터는 자동 rewrite되지 않는다** ([Delta Lake][1])

그래서 키 변경 직후에는 테이블 내부에

* “옛 키로 클러스터링된 파일(=옛 ZCube들)”
* “새 키로 쓰인 신규 파일”
  이 섞이게 되고, 이 상태에선 새 키 필터의 data skipping 효과가 기대만큼 안 나올 수 있습니다.

이때 `OPTIMIZE FULL`이 “한 번 전체를 현재 키에 맞게 재정렬”해 줍니다. ([Delta Lake][1])

---

## 6) `OPTIMIZE FULL` 동작을 “실행 단계”로 쪼개보기

아래는 **구현을 과도하게 단정하지 않으면서**, 문서/프로토콜에서 보장되는 사실을 기반으로 *어떻게 굴러갈 수밖에 없는지*를 정리한 것입니다.

### 6.1 입력: 현재 클러스터링 키를 어디서 읽나?

* `_delta_log`의 **domainMetadata(domain=`delta.clustering`)**에 기록된 “현재 클러스터링 컬럼 목록”을 사용합니다. ([GitHub][2])

### 6.2 파일 선정: `OPTIMIZE` vs `OPTIMIZE FULL`

* `OPTIMIZE`는 보통 “점진적 유지” 관점에서 **일부 파일만** 대상으로 삼습니다(예: 새로 유입된 파일, 작은 파일, 품질이 낮은 파일 등).
* `OPTIMIZE FULL`은 문서상 “**모든 레코드를 강제로 reclustering**”하는 모드이므로, **기존에 클러스터링되어 있던 데이터까지 포함**해 현재 키 기준으로 필요하면 재작성합니다. ([Delta Lake][1])

> 문서에 “reclusters all existing data **as necessary**”라고 표현한 것은
> 구현이 “이미 완전히 현재 키 기준으로 정렬된 영역은 굳이 또 안 만질 수도” 있음을 암시합니다.
> 그리고 “클러스터링 컬럼 변화가 없으면 OPTIMIZE FULL이 OPTIMIZE처럼 동작”한다고 명시합니다. ([Delta Lake][1])

### 6.3 재작성: 결과 파일을 어떻게 만들까?

결과적으로는:

* 선택된 입력 파일들을 읽고
* 클러스터링 키 기준 locality를 높이는 방식으로 레코드를 재배치한 뒤
* 적정 파일 크기(타겟 사이즈)에 맞춰 새 Parquet 파일들을 씁니다.

그리고 새 파일의 `add` 액션에는

* `clusteringProvider`가 세팅되고 ([GitHub][2])
* ZCube 관련 태그(예: ZCUBE_ID 등)가 달릴 수 있습니다.

### 6.4 커밋(로그 결과물): 어떤 구조가 남나?

Delta는 ACID 로그 기반이므로, `OPTIMIZE FULL` 결과는 항상 다음 형태로 남습니다.

* commitInfo(operation = OPTIMIZE …)
* `remove` 액션들: 기존 파일 제거 표시
* `add` 액션들: 새 파일 추가
* 그리고 clustered table이라면 domainMetadata / clusteringProvider 규칙을 만족해야 함 ([GitHub][2])

---

## 7) “결과물 구조”를 `_delta_log`에서 직접 확인하는 법

아래는 “어디를 보면 Liquid Clustering / OPTIMIZE FULL 흔적이 있나”를 보여주는 *관찰 포인트*입니다. (예시는 개념용)

### 7.1 domainMetadata에서 클러스터링 키 확인

Delta 프로토콜이 요구하는 부분입니다. ([GitHub][2])

```json
{
  "domainMetadata": {
    "domain": "delta.clustering",
    "configuration": {
      "clusteringColumns": ["colA", "colB"]
    }
  }
}
```

### 7.2 add 액션에서 clusteringProvider / ZCube 태그 확인

프로토콜의 `clusteringProvider` 요구사항 ([GitHub][2]) 과, ZCube 태그 예시  를 함께 보면 감이 잡힙니다.

```json
{
  "add": {
    "path": "part-00000-....snappy.parquet",
    "size": 123456789,
    "stats": "{...minValues/maxValues...}",
    "clusteringProvider": "liquid", 
    "tags": {
      "ZCUBE_ID": "....",
      "ZCUBE_ZORDER_BY": "colA,colB",
      "ZCUBE_ZORDER_CURVE": "hilbert"
    }
  }
}
```

> 여기서 **stats(min/max)**가 좋아질수록, colA/colB 필터 시 *data skipping*이 강해지는 구조입니다.
> Liquid Clustering은 결국 “이 stats를 쿼리에 유리하게 만들도록 파일을 재배치”하는 메커니즘이라고 보면 됩니다.

---

## 8) Delta Lake 3.3에서 “강화된 Liquid Clustering” 포인트 요약

Delta Lake 3.3의 릴리스/문서에서 Liquid Clustering 관련 하이라이트로 반복 등장하는 건 크게 두 가지입니다.

1. **`OPTIMIZE FULL` 지원**: Liquid 테이블을 “완전 재클러스터링” ([delta.io][5])
2. **기존(특히 unpartitioned) 테이블에 `ALTER TABLE ... CLUSTER BY (...)`로 Liquid Clustering 켜기** ([GitHub][6])

---

## 9) Delta Lake 4.x에서의 언급(무엇이 바뀌었나?)

Delta Lake 4.0 프리뷰 릴리스 노트에서도 Liquid Clustering 업데이트로

* `OPTIMIZE FULL` 지원,
* 기존 테이블에 CLUSTER BY 적용
  을 다시 강조합니다. ([GitHub][6])

즉, **3.3에서 들어온 핵심 운영 기능(특히 OPTIMIZE FULL)이 4.x 계열에서도 중요한 축으로 유지**된다고 보면 됩니다.

---

## 10) 운영 관점에서 `OPTIMIZE` vs `OPTIMIZE FULL` 선택 가이드

### 10.1 `OPTIMIZE`를 주로 쓰는 경우

* 클러스터링 키가 안정적이고(바뀌지 않고)
* 신규 데이터가 계속 들어오며
* 점진적으로 레이아웃을 유지하면 충분한 경우

### 10.2 `OPTIMIZE FULL`을 “명확히” 써야 하는 경우

문서가 가장 강하게 말하는 경우는 이겁니다.

* **클러스터링 컬럼을 변경한 직후**

  * 기존 데이터는 자동으로 새 키로 재배치되지 않기 때문에,
  * “현재 키 기준 레이아웃”을 보장하려면 `OPTIMIZE FULL`을 수행 ([Delta Lake][1])

또한 대규모 테이블에서 `OPTIMIZE FULL`은 오래 걸 수 있다고 경고합니다. ([Delta Lake][1])

---

## 11) 당신 케이스에서 “개념이 안 잡힐 때”의 한 줄 요약(비유)

* Liquid Clustering을 **“파티션 디렉터리를 만드는 대신, 파일 단위의 ‘정렬/군집화 품질’을 관리하는 시스템”**이라고 생각하세요.
* ZCube는 **그 파일 군집화 작업의 배치(세대) ID** 같은 것.
* `OPTIMIZE`는 **부분 수선**, `OPTIMIZE FULL`은 **키 변경 후 전체 재배치(필요 시)** 입니다.

---

원하면 다음을 바로 이어서 더 깊게 파고들 수 있어요(추가 질문 없이 제가 이어서 써도 됩니다):

1. **Liquid Clustering에서 “클러스터링 품질”을 어떻게 판단할 수 있는지**(메트릭/로그 관찰 포인트)
2. **클러스터링 키 선정 전략**(파티션 키/고카디널리티/조인키/시간 컬럼을 어떻게 다루는 게 안전한지)
3. Spark/Trino 관점에서 **data skipping이 실제로 어떻게 체감 성능으로 이어지는지**(파일 min/max, predicate pushdown, split planning까지)

원하시면, 지금 쓰고 있는 테이블 스키마(주요 쿼리의 where/join 조건 포함)를 예로 들어서 **“이 키가 왜 유리/불리한지”**까지 Liquid Clustering 관점으로 구체 평가해 드릴게요.

[1]: https://docs.delta.io/delta-clustering/?utm_source=chatgpt.com "Use liquid clustering for Delta tables"
[2]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[3]: https://docs.delta.io/optimizations-oss/?utm_source=chatgpt.com "Optimizations"
[4]: https://docs.databricks.com/aws/en/delta/optimize?utm_source=chatgpt.com "Optimize data file layout | Databricks on AWS"
[5]: https://delta.io/blog/delta-lake-3-3/?utm_source=chatgpt.com "Delta Lake 3.3"
[6]: https://github.com/delta-io/delta/releases?utm_source=chatgpt.com "Releases · delta-io/delta"
