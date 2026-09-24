# คู่มือ: ให้ APzzzz-WWWWW มี dag-processor ของตัวเอง (bundle `pilot`, Airflow 3 + Helm)

> **ชื่อในคู่มือนี้เป็นตัวแทน (placeholder)** ให้แทนด้วยค่าจริง
>
> | placeholder | ความหมาย |
> |---|---|
> | `<company_name>` | folder ชื่อบริษัทใต้ `/opt/airflow/dags` |
> | `APzzzz-WWWWW` | app ที่จะแยกออกมา (bundle `pilot`) |
> | `<chart-version>` | เวอร์ชันของ chart `apache-airflow/airflow` ที่ใช้อยู่ |
>
> path ของไฟล์ DAG: `/opt/airflow/dags/<company_name>/APzzzz-WWWWW/<env>_<company_name>_apzzzz_<งาน>.py`

## เป้าหมาย

- DAG ของ **APzzzz-WWWWW** ถูก parse โดย **dag-processor ของตัวเอง** ไม่ต้องรอคิวรวมกับ app อื่น
- dag-processor **default** (2 replicas) ยังทำงานต่อตามเดิม ดูแลทุก app ยกเว้น APzzzz-WWWWW
- ทุกอย่าง deploy ด้วย **`helm upgrade`** ครั้งเดียว ย้อนกลับได้

```
ตอนนี้                                      หลังทำ
┌──────────────────────────────┐          ┌──────────────────────────────────────────┐
│ dag-processor (2 replicas)   │          │ dag-processor (2 replicas, เดิม)          │
│ ทุก app อยู่คิวเดียว              │   ──►    │  --bundle-name dags-folder   ทุก app อื่น  │
│ รอบละ ~75–92 นาที              │          ├──────────────────────────────────────────┤
└──────────────────────────────┘          │ dag-processor-pilot (1 replica, ใหม่)     │
                                          │  --bundle-name pilot   APzzzz-WWWWW      │
                                          │  4 processes, รอบละ ~9 นาที               │
                                          └──────────────────────────────────────────┘
```

| | DAG (active) | เวลา parse รวม | รอบที่คาดไว้ |
|---|---|---|---|
| `pilot` (APzzzz-WWWWW) | 30 | 0.60 ชม. (median 88 วินาที/ไฟล์) | ~9 นาที (4 processes) |
| `dags-folder` (default) | ~1,500 | ~20.9 ชม. | เท่าเดิม ~75–90 นาที |

ตัวเลขมาจาก export วันที่ 2026-09-23 18:14 การแยก `pilot` ออกมาไม่ได้ทำให้ default เร็วขึ้น เพราะ `pilot` มีเวลา parse แค่ 3% ของทั้งหมด งานนี้มีไว้**พิสูจน์ว่าเวลาที่รอเกิดจากคิวรวม** ถ้าได้ผลค่อยแยก app ที่หนักตามไป

## ทำไมตั้งชื่อ bundle อย่างเดียวไม่พอ

bundle เป็นแค่การแบ่งกลุ่ม ถ้า dag-processor ไม่ได้ระบุ `--bundle-name` มันจะ parse **ทุก bundle** ในคิวเดียวกัน รอบจึงยาวเท่าเดิม ต้องมีครบ 4 อย่างนี้:

| # | สิ่งที่ต้องมี | ทำในขั้นตอน | ถ้าขาด |
|---|---|---|---|
| 1 | `dag_bundle_config_list` มี bundle `pilot` และตั้งให้**ทุก component** | 2 | worker หาไฟล์ของ DAG ใน `pilot` ไม่เจอตอนรัน task |
| 2 | processor default รัน `--bundle-name dags-folder` | 2 | processor ที่ไม่ระบุ bundle จะ parse ทุก bundle `pilot` ยังเร็ว เพราะมี pod ของตัวเอง แต่ default จะ parse ไฟล์ของ `pilot` ซ้ำในคิวรวม |
| 3 | deployment แยกรัน `--bundle-name pilot` | 3 | ไม่มี processor ของ `pilot` โดยเฉพาะ |
| 4 | `.airflowignore` ที่ root ตัด folder ของ app ออกจาก `dags-folder` | 5 | ไฟล์ถูก parse 2 ครั้ง และ `bundle_name` ของ DAG สลับไปมา |

---

## ขั้นตอนที่ 0: ตรวจก่อนเริ่ม (ไม่เปลี่ยนแปลงอะไรในระบบ)

```sh
NS=airflow
RELEASE=airflow

helm version --short                                   # ต้องเป็น Helm 3.x (ดูหมายเหตุขั้นตอนที่ 3)
helm -n $NS list                                       # ดูเวอร์ชันของ chart → ใช้เป็น <chart-version>
helm -n $NS get values $RELEASE -o yaml > values-current.yaml   # สำรอง values ปัจจุบัน
which yq && yq --version                               # ต้องเป็น mikefarah/yq v4

kubectl -n $NS exec deploy/airflow-dag-processor -- airflow version
kubectl -n $NS exec deploy/airflow-dag-processor -- airflow dag-processor --help | grep -i bundle   # ต้องมี --bundle-name
kubectl -n $NS exec deploy/airflow-dag-processor -- airflow config get-value dag_processor dag_bundle_config_list
kubectl -n $NS exec deploy/airflow-dag-processor -- airflow config get-value core dag_ignore_file_syntax
kubectl -n $NS exec deploy/airflow-dag-processor -- find /opt/airflow/dags -name .airflowignore
kubectl -n $NS get deploy airflow-dag-processor \
  -o jsonpath='{range .spec.template.spec.containers[*]}{.name}{"\n"}{end}'   # ต้องมี container ชื่อ dag-processor
```

ถ้าเคยทดลองแก้ด้วย `kubectl` มาก่อน (เช่น `kubectl set env` หรือสร้าง deployment ด้วยมือ) ให้ลบของเหล่านั้นก่อน `helm upgrade` จะไม่ลบ env ที่เพิ่มด้วย `kubectl set env` ให้

**baseline:** DAG ของ app ถูก parse ครั้งล่าสุดนานสุดกี่นาทีแล้ว (เก็บไว้เทียบในขั้นตอนที่ 6)

```sql
SELECT count(*) AS dags,
       round(extract(epoch FROM now() - min(last_parsed_time)) / 60) AS oldest_parse_min
FROM dag
WHERE NOT is_stale AND fileloc LIKE '%/<company_name>/APzzzz-WWWWW/%';
```

## ขั้นตอนที่ 1: ทดสอบว่า DAG ของ app parse ได้เมื่อเริ่มจาก folder ของ app

bundle `pilot` เริ่มสแกนที่ folder ของ app จุดที่พังบ่อยคือ **import โค้ดร่วม**ที่อ้างจาก `/opt/airflow/dags` ทดสอบใน pod เดิมได้เลย

```sh
kubectl -n airflow exec -it deploy/airflow-dag-processor -- python - <<'PY'
try:
    from airflow.dag_processing.dagbag import DagBag   # Airflow 3.1+
except ImportError:
    from airflow.models.dagbag import DagBag
bag = DagBag("/opt/airflow/dags/<company_name>/APzzzz-WWWWW", include_examples=False)
print("dags:", len(bag.dags), "import errors:", len(bag.import_errors))
for f, e in list(bag.import_errors.items())[:5]:
    print("  ", f, e.strip().splitlines()[-1])
PY
```

- ผลที่ควรได้: **~30 DAG และไม่มี import error** (ใช้เวลาประมาณ 45 นาที เพราะ parse ทีละไฟล์)
- ถ้ามี `ModuleNotFoundError` ให้เปิด `PYTHONPATH` ในขั้นตอนที่ 2 แล้วทดสอบซ้ำด้วย `PYTHONPATH=/opt/airflow/dags python - <<'PY' ...`
- ถ้าจำนวน DAG มากกว่าเดิม หรือมีไฟล์ใน `utils/`, `tests/` ถูก parse ให้ดูหัวข้อ "`.airflowignore` ของแต่ละ bundle"

## ขั้นตอนที่ 2: `values-pilot.yaml`

```yaml
config:
  dag_processor:
    # ใส่ใน config: เพื่อให้ทุก component (scheduler, api-server, worker, triggerer, dag-processor) ได้ค่าเดียวกัน
    dag_bundle_config_list: >-
      [
        {"name": "dags-folder",
         "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
         "kwargs": {"path": "/opt/airflow/dags"}},
        {"name": "pilot",
         "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
         "kwargs": {"path": "/opt/airflow/dags/<company_name>/APzzzz-WWWWW"}}
      ]

# processor default (2 replicas เดิม) ดูแลเฉพาะ dags-folder
dagProcessor:
  args: ["bash", "-c", "exec airflow dag-processor --bundle-name dags-folder"]

# ใส่เฉพาะถ้าขั้นตอนที่ 1 พบ ModuleNotFoundError
# env:
#   - name: PYTHONPATH
#     value: /opt/airflow/dags
```

- ชื่อ `dags-folder` และ path `/opt/airflow/dags` ต้องตรงกับค่าเดิมที่เห็นในขั้นตอนที่ 0 DAG ของ app อื่นจึงไม่ต้องย้าย bundle
- ถ้า `values-current.yaml` มี `config.dag_processor` หรือ `env:` อยู่แล้ว ให้**รวม**เข้าไปในไฟล์นี้ Helm เขียนทับ list ทั้งก้อน ไม่ได้ต่อท้าย

## ขั้นตอนที่ 3: สคริปต์สร้าง `dag-processor-pilot` (Helm post-renderer)

chart ทางการสร้าง dag-processor ได้ deployment เดียว สคริปต์นี้รับ manifest ที่ Helm render แล้ว คัดลอก deployment ของ dag-processor มาแก้เป็นของ `pilot` แล้วส่งกลับให้ Helm ข้อดี:
- pod ของ `pilot` ได้ volume, secret และ env ครบเหมือน default
- deployment ใหม่เป็นส่วนหนึ่งของ Helm release `helm upgrade` / `helm rollback` จัดการให้ และได้ของใหม่ทุกครั้งที่อัปเกรด chart

บันทึกเป็น `add-pilot-processor.sh` ไว้ข้าง values แล้ว `chmod +x add-pilot-processor.sh`

```sh
#!/bin/sh
# Helm post-renderer: add a dag-processor Deployment dedicated to one DAG bundle
set -eu
BUNDLE=pilot
PROCS=4

manifests=$(mktemp)
trap 'rm -f "$manifests"' EXIT
cat > "$manifests"

cat "$manifests"
printf '\n---\n'
BUNDLE=$BUNDLE PROCS=$PROCS yq '
  select(.kind == "Deployment" and .metadata.labels.component == "dag-processor")
  | .metadata.name += "-" + strenv(BUNDLE)
  | .metadata.labels.component = "dag-processor-" + strenv(BUNDLE)
  | .spec.replicas = 1
  | .spec.selector.matchLabels.component = "dag-processor-" + strenv(BUNDLE)
  | .spec.template.metadata.labels.component = "dag-processor-" + strenv(BUNDLE)
  | (.spec.template.spec.containers[] | select(.name == "dag-processor")) |= (
      .args = ["bash", "-c", "exec airflow dag-processor --bundle-name " + strenv(BUNDLE)]
      | .env += [{"name": "AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES", "value": strenv(PROCS)}]
    )
' "$manifests"
```

- **ต้องเปลี่ยน label `component`:** ไม่อย่างนั้น deployment default จะนับ pod ของ `pilot` เป็นของตัวเอง
- **`replicas = 1`:** ยังไม่รู้ว่าหลาย replica แบ่งงานกันจริงไหม (ดูหัวข้อ "dag-processor default มี 2 replicas") ถ้าอยากเร็วขึ้นให้เพิ่ม `PROCS` แทน
- **Helm 4:** `--post-renderer` ต้องเป็น plugin แทนสคริปต์ ถ้าใช้ Helm 4 ให้ห่อสคริปต์เป็น post-renderer plugin หรือใช้วิธีในภาคผนวก

ตรวจผลของสคริปต์ก่อน deploy ได้ (ไม่เปลี่ยนแปลงอะไรในระบบ):

```sh
helm template $RELEASE apache-airflow/airflow -n $NS --version <chart-version> \
  -f values-current.yaml -f values-pilot.yaml --post-renderer ./add-pilot-processor.sh \
  | yq 'select(.kind == "Deployment" and (.metadata.labels.component | test("^dag-processor")))
        | [.metadata.name, .spec.replicas, (.spec.template.spec.containers[] | select(.name == "dag-processor") | .args[-1])]'
```

ต้องเห็น 2 deployment: `airflow-dag-processor` (2 replicas, `--bundle-name dags-folder`) และ `airflow-dag-processor-pilot` (1 replica, `--bundle-name pilot`)

## ขั้นตอนที่ 4: deploy

> ⚠ ทำใน**ช่วงที่ DAG ของ app ไม่มีรอบรัน** เพราะขั้นตอนที่ 5 อาจทำให้ DAG บางตัวเป็น stale ได้ไม่เกิน 1 รอบของ `pilot` (~10 นาที) และ pod ทุก component จะ restart เพราะ config เปลี่ยน

```sh
helm diff upgrade $RELEASE apache-airflow/airflow -n $NS --version <chart-version> \
  -f values-current.yaml -f values-pilot.yaml --post-renderer ./add-pilot-processor.sh   # ต้องมี plugin helm-diff

helm upgrade $RELEASE apache-airflow/airflow -n $NS --version <chart-version> \
  -f values-current.yaml -f values-pilot.yaml --post-renderer ./add-pilot-processor.sh

kubectl -n $NS rollout status deploy/airflow-dag-processor
kubectl -n $NS rollout status deploy/airflow-dag-processor-pilot
```

**ต้องใส่ `--post-renderer ./add-pilot-processor.sh` ทุกครั้งที่ `helm upgrade`** จากนี้ไป ถ้าลืม Helm จะลบ `dag-processor-pilot` และ DAG ของ app จะไม่มีใคร parse

## ขั้นตอนที่ 5: ตัด app ออกจาก `dags-folder` (`.airflowignore`)

รอให้ `dag-processor-pilot` parse ครบ 1 รอบก่อน (**~15 นาทีหลังขั้นตอนที่ 4**) แล้วเพิ่มบรรทัดนี้ใน `.airflowignore` ที่ root ของ blob (`/opt/airflow/dags/.airflowignore`) ถ้ามีไฟล์อยู่แล้ว ให้**เพิ่มบรรทัด** อย่าเขียนทับ

```
<company_name>/APzzzz-WWWWW/
```

- เขียนแบบนี้ใช้ได้ทั้ง syntax `glob` (ค่าเริ่มต้นของ Airflow 3.x) และ `regexp` ไม่ต้องมี `/**` และห้ามมี `/` นำหน้า
- **ทำไมต้องรอ:** ถ้าตัดก่อนที่ `pilot` จะ parse DAG ครบ `dags-folder` จะเห็นว่าไฟล์หายไปแล้วตั้ง DAG ที่ยังเป็น `bundle_name = dags-folder` เป็น stale จนกว่า `pilot` จะ parse ถึง ในช่วงรอ ทั้งสอง processor parse ไฟล์เดียวกัน `bundle_name` จึงสลับไปมาได้ ถือว่าปกติ

## ขั้นตอนที่ 6: ตรวจและวัดผล (หลังขั้นตอนที่ 5 ประมาณ 15–30 นาที)

```sql
-- 1) DAG ของ app ต้องอยู่ bundle pilot ทั้งหมด และไม่เป็น stale  →  pilot | false | ~30
SELECT bundle_name, is_stale, count(*) FROM dag
WHERE fileloc LIKE '%/<company_name>/APzzzz-WWWWW/%' GROUP BY 1, 2;

-- 2) รอบของแต่ละ bundle  →  pilot ไม่เกิน ~10 นาที
SELECT bundle_name, count(*) AS dags,
       round(extract(epoch FROM now() - min(last_parsed_time)) / 60) AS oldest_parse_min
FROM dag WHERE NOT is_stale GROUP BY 1 ORDER BY 1;
```

```sh
kubectl -n $NS logs deploy/airflow-dag-processor-pilot --since=30m | grep -iE 'error|bundle' | tail -20
```

- Import Errors ใน UI ต้องไม่มีรายการใหม่
- trigger DAG ของ app 1 ตัว แล้วดูว่า task รันผ่าน (แปลว่า worker หาไฟล์ใน bundle `pilot` เจอ)
- **ทดสอบจริง:** แก้ `doc_md` ของ DAG ใน app แล้ว deploy จับเวลาจน version ใหม่ขึ้นใน UI เทียบกับ baseline

| ผล | แปลว่า |
|---|---|
| `pilot` รอบไม่เกิน ~10 นาที และ deploy ขึ้น UI ภายในไม่กี่นาที | เวลาที่รอเกิดจาก**คิวรวม** การแยก bundle ได้ผล แยก app อื่นตามได้ |
| `pilot` ยังช้าใกล้เคียงเดิม | คอขวดอยู่ที่อื่น เช่น การ sync จาก blob หรือระบบภายนอกที่ DAG เรียกตอน parse |
| DAG ของ app เป็น stale หรือมี import error | ดูหัวข้อ "แก้ปัญหา" ถ้าแก้ไม่ได้ให้ rollback |

---

## Rollback

**แบบปกติ** (DAG ไม่ stale ระหว่างย้อนกลับ):

```sh
# 1) หยุด processor ของ pilot ชั่วคราว (ขั้นตอนที่ 4 จะลบ deployment ให้เอง)
kubectl -n $NS scale deploy/airflow-dag-processor-pilot --replicas=0
# 2) ลบบรรทัด <company_name>/APzzzz-WWWWW/ ออกจาก .airflowignore บน blob
# 3) รอจน dags-folder parse DAG ของ app ครบ (~1 รอบของ default, 75–90 นาที)
#    SQL ข้อ 1 ในขั้นตอนที่ 6 ต้องได้ dags-folder | false | ~30
# 4) deploy values เดิม โดยไม่ใส่ post-renderer
helm upgrade $RELEASE apache-airflow/airflow -n $NS --version <chart-version> -f values-current.yaml
```

**แบบเร่งด่วน:** ทำข้อ 2 และข้อ 4 ทันที DAG ของ app อาจ stale หรือรัน task ไม่ได้จนกว่า `dags-folder` จะ parse ถึง (ไม่เกิน 1 รอบของ default)

## แก้ปัญหา

| อาการ | สาเหตุที่น่าจะเป็น |
|---|---|
| `pilot` ยังรอบละ ~75–90 นาที | ไม่มี `dag-processor-pilot` (ลืม `--post-renderer`) หรือ pod ของ `pilot` ไม่ได้รัน `--bundle-name pilot` |
| `pilot` เร็วแล้ว แต่ log ของ pod default ยังมีไฟล์ของ app | default ไม่ได้ระบุ `--bundle-name dags-folder` จึง parse ทุก bundle รวม `pilot` ซ้ำ (`.airflowignore` ที่ root ช่วยไม่ได้ เพราะ `pilot` เป็นอีก bundle) |
| `bundle_name` ของ DAG สลับระหว่าง `dags-folder` กับ `pilot` หลังขั้นตอนที่ 5 | บรรทัดใน `.airflowignore` ไม่ match ตรวจ path และ `dag_ignore_file_syntax` |
| DAG ของ app เป็น stale ทั้งที่ไฟล์ยังอยู่ | `dag-processor-pilot` ไม่ทำงาน หรือ path ใน `kwargs` ผิด |
| task ของ DAG ใน `pilot` fail ว่าหาไฟล์หรือ bundle ไม่เจอ | worker ไม่มี `dag_bundle_config_list` (ต้องอยู่ใน `config:` ไม่ใช่ env ของ dag-processor อย่างเดียว) |
| import error ใหม่ หรือมีไฟล์ใน `utils/`, `tests/` ถูก parse | pattern ใน `.airflowignore` ของ folder แม่ไม่มีผลกับ `pilot` แล้ว (ดูหัวข้อถัดไป) |
| `ModuleNotFoundError` | เปิด `PYTHONPATH=/opt/airflow/dags` ใน `values-pilot.yaml` |

ตรวจสิ่งที่ deploy อยู่จริง:

```sh
kubectl -n $NS get deploy -l 'component in (dag-processor,dag-processor-pilot)' \
  -o custom-columns='NAME:.metadata.name,REPLICAS:.spec.replicas,ARGS:.spec.template.spec.containers[0].args'
for c in scheduler api-server triggerer worker dag-processor dag-processor-pilot; do
  pod=$(kubectl -n $NS get pods -l component=$c -o name | head -1)
  [ -n "$pod" ] && echo "== $c" && kubectl -n $NS exec "$pod" -- \
    airflow config get-value dag_processor dag_bundle_config_list
done
```

---

## เรื่องที่ควรรู้

### `.airflowignore` ของแต่ละ bundle

แต่ละ processor อ่านเฉพาะ `.airflowignore` **ที่อยู่ใต้ root ของ bundle ตัวเอง** pattern ในแต่ละไฟล์เขียนเทียบกับ folder ที่ไฟล์นั้นอยู่

| processor | root ของ bundle | `.airflowignore` ที่อ่าน |
|---|---|---|
| default (`dags-folder`) | `/opt/airflow/dags` | ไฟล์ที่ root และในทุก folder ย่อยที่สแกน |
| `pilot` | `/opt/airflow/dags/<company_name>/APzzzz-WWWWW` | เฉพาะไฟล์ใน folder ของ app ลงไป **ไม่อ่าน**ไฟล์ที่ root หรือที่ `<company_name>/` |

- บรรทัด `<company_name>/APzzzz-WWWWW/` ที่ root จึงตัด app ออกจาก default ได้ โดยไม่กระทบ `pilot`
- pattern เดิมที่เคยซ่อนไฟล์ใน folder ของ app จะไม่มีผลกับ `pilot` อีก ให้ย้ายไปไว้ใน `<company_name>/APzzzz-WWWWW/.airflowignore` โดยเขียนเทียบกับ folder ของ app:

  ```
  # /opt/airflow/dags/.airflowignore (เดิม)      → <company_name>/APzzzz-WWWWW/.airflowignore (ใหม่)
  <company_name>/APzzzz-WWWWW/utils/              → utils/
  <company_name>/APzzzz-WWWWW/**/*_test.py        → **/*_test.py
  ```

- `[core] dag_ignore_file_syntax` ของ Airflow 3.x มีค่าเริ่มต้นเป็น `glob` (รูปแบบเดียวกับ `.gitignore`) Airflow 2.x เป็น `regexp` ให้ใช้ค่าที่ตรวจได้ในขั้นตอนที่ 0

### dag-processor default มี 2 replicas

รอบที่วัดได้ตอนนี้ (75–92 นาที) ตรงกับที่ได้จาก **16 processes** (80 นาที) ไม่ใช่ 32 (40 นาที) แปลว่า replica ที่ 2 ไม่ได้ทำให้รอบสั้นลงครึ่งหนึ่ง ยังไม่ได้ยืนยันว่าเป็นเพราะอะไร:

| สมมติฐาน | วิธีตรวจ | ถ้าใช่ |
|---|---|---|
| **H1:** 2 pods parse ไฟล์ชุดเดียวกันซ้ำ | ไฟล์เดียวกันขึ้นใน log ของทั้ง 2 pods ห่างกันไม่ถึง 1 รอบ | replica ที่ 2 ไม่ได้ช่วยให้เร็วขึ้น ลดเหลือ 1 แล้วเอา CPU ไปให้ processor ของ bundle ใหม่ |
| **H2:** แบ่งงานกัน แต่ CPU ไม่พอ | `kubectl top pod` ใช้ CPU เต็ม limit ตลอด | ต้องเพิ่ม CPU ก่อน แยก bundle อย่างเดียวจะไม่เร็วขึ้น |

```sh
FILE=<ไฟล์ DAG ใดก็ได้ของ app อื่น>.py
for p in $(kubectl -n $NS get pods -l component=dag-processor -o name); do
  echo "== $p"; kubectl -n $NS logs "$p" --since=3h | grep -F "$FILE" | tail -3
done
kubectl -n $NS get pods -l component=dag-processor -o wide
kubectl -n $NS top pod -l component=dag-processor
```

งานในคู่มือนี้ไม่ได้เปลี่ยนจำนวน replica ของ default ส่วน `pilot` ใช้ 1 replica

### วัดผลด้วยรายงาน

รัน export ซ้ำทุก 10–15 นาทีสัก 2–3 ชม. แล้วสร้างรายงานด้วย `./generate-report.sh` รายงานอ่านคอลัมน์ `bundle_name` ได้ ให้ตั้ง `PARSING_PROCESSES` เท่ากับจำนวน process ที่ทำงานได้จริง (ค่าเริ่มต้น 32 ถือว่า 2 replicas แบ่งงานกัน ถ้าเป็น H1 ให้ใช้ 16) ไม่อย่างนั้นรอบที่คาดไว้จะดูเร็วกว่าที่เป็นจริง

### ขั้นต่อไป (ถ้า `pilot` ได้ผล)

- แยก app ที่ใช้เวลา parse มาก (ดูส่วน "By application" ในรายงาน) เพิ่ม bundle ใน `dag_bundle_config_list` เพิ่มบรรทัดใน `.airflowignore` ที่ root และเพิ่ม deployment ในสคริปต์ post-renderer (ทำซ้ำคำสั่ง `yq` ด้วย `BUNDLE` และ `PROCS` ของ app นั้น)
- แก้ต้นเหตุที่ DAG หลายตัวใช้ประมาณ 90 วินาทีต่อไฟล์ ได้ผลมากกว่าการแยก bundle (หาจุดที่ช้าด้วย `python -m cProfile -s cumtime <ไฟล์ DAG>` ใน pod ของ dag-processor)
- ให้ CI/CD เรียก `PUT /api/v2/parseDagFile/{file_token}` หลัง deploy เพื่อให้ไฟล์ที่แก้ได้ parse ก่อนไฟล์อื่น

---

## ภาคผนวก: ไม่ใช้ post-renderer

ใช้เมื่อเป็น Helm 4 หรือใช้ post-renderer ไม่ได้ render deployment ของ dag-processor จาก chart แล้วสร้างไฟล์ของ `pilot` เอง:

```sh
helm template $RELEASE apache-airflow/airflow -n $NS --version <chart-version> \
  -f values-current.yaml -f values-pilot.yaml \
  --show-only templates/dag-processor/dag-processor-deployment.yaml \
  | ./add-pilot-processor.sh \
  | yq 'select(.metadata.name == "airflow-dag-processor-pilot")' > dag-processor-pilot.yaml

helm upgrade $RELEASE apache-airflow/airflow -n $NS --version <chart-version> \
  -f values-current.yaml -f values-pilot.yaml
kubectl -n $NS apply -f dag-processor-pilot.yaml
```

deployment นี้ไม่อยู่ใน Helm release ต้อง render ใหม่ทุกครั้งที่อัปเกรด chart และตอน rollback ต้องลบเองด้วย `kubectl -n $NS delete -f dag-processor-pilot.yaml` ตรวจ path ของ template ด้วย `helm template ... | grep '^# Source: .*dag-processor'`

## สมมติฐานที่ยังไม่ได้ทดสอบกับเวอร์ชันที่ใช้จริง

คู่มือนี้อ้างอิงจากความรู้เรื่อง Airflow 3 และ chart ทางการ ให้ยืนยันใน sandbox (เช่น k3s + Airflow 3.2.1 พร้อม DAG ตัวอย่าง 2–3 ไฟล์) หรือบน dev ก่อน:

- `airflow dag-processor --bundle-name` ทำให้แต่ละ pod parse เฉพาะ bundle ของตัวเอง
- `.airflowignore` ที่ root ไม่มีผลกับ bundle ที่เริ่มที่ folder ย่อย แต่ `.airflowignore` ใน folder ของ app มีผล
- ตอนย้าย DAG ข้าม bundle ประวัติ run และ version ยังอยู่ (เพราะ `dag_id` เดิม) และไม่มี DAG ค้างเป็น stale
- ชื่อ container (`dag-processor`), label `component` และ path ของ template ตรงกับ chart เวอร์ชันที่ใช้
- task ของ DAG ใน bundle ใหม่รันบน worker ได้
