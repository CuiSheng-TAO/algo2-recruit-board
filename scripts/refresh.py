#!/usr/bin/env python3
"""Fetch Feishu Hire data and generate data.js. Designed for GitHub Actions."""

import requests
import json
import time
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(line_buffering=True)

APP_ID = os.environ["FEISHU_APP_ID"]
APP_SECRET = os.environ["FEISHU_APP_SECRET"]
JOB_ID = os.environ.get("FEISHU_JOB_ID", "7564259933761423643")
BASE_URL = "https://open.feishu.cn/open-apis"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STAGE_IDS = [
    "7483922292361365798",  # 0: 简历初筛
    "7483922292361382182",  # 1: 简历评估
    "7487173396766968091",  # 2: 初面邀约
    "7483922292361398566",  # 3: 专业一面
    "7484986929462872370",  # 4: 专业二面
    "7484987042608285962",  # 5: HR面
    "7483922292361414950",  # 6: Offer沟通
    "7483922292361431334",  # 7: 待入职
    "7483922292361447718",  # 8: 已入职
]
STAGE_INDEX = {sid: i for i, sid in enumerate(STAGE_IDS)}
INTERVIEW_STAGES = set(STAGE_IDS[3:])  # 专业一面 onward


def get_token():
    resp = requests.post(f"{BASE_URL}/auth/v3/tenant_access_token/internal", json={
        "app_id": APP_ID, "app_secret": APP_SECRET
    })
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Token error: {data}")
    return data["tenant_access_token"]


def list_application_ids(token):
    headers = {"Authorization": f"Bearer {token}"}
    app_ids = []
    page_token = None
    while True:
        params = {"job_id": JOB_ID, "page_size": 20}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(f"{BASE_URL}/hire/v1/applications", headers=headers, params=params)
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"List error: {data.get('msg')}")
        app_ids.extend(data.get("data", {}).get("items", []))
        if not data.get("data", {}).get("has_more"):
            break
        page_token = data["data"].get("page_token")
    return app_ids


def fetch_app_detail(args):
    app_id, token = args
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(3):
        try:
            resp = requests.get(f"{BASE_URL}/hire/v1/applications/{app_id}",
                                headers=headers, timeout=10)
            data = resp.json()
            if data.get("code") == 0:
                return data["data"]["application"]
            elif data.get("code") == 99991400:
                time.sleep(1 + attempt)
                continue
            else:
                return {"id": app_id, "error": data.get("msg")}
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
            else:
                return {"id": app_id, "error": str(e)}
    return {"id": app_id, "error": "max retries"}


def fetch_talent_name(talent_id, token):
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(f"{BASE_URL}/hire/v1/talents/{talent_id}", headers=headers, timeout=10)
        data = resp.json()
        if data.get("code") == 0:
            return data["data"]["talent"].get("basic_info", {}).get("name", "")
    except Exception:
        pass
    return ""


def batch_fetch_details(app_ids, token):
    all_details, errors = [], []
    batch_size = 10
    for i in range(0, len(app_ids), batch_size):
        batch = app_ids[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=batch_size) as ex:
            futures = {ex.submit(fetch_app_detail, (aid, token)): aid for aid in batch}
            for fut in as_completed(futures):
                r = fut.result()
                if r and "error" not in r:
                    all_details.append(r)
                else:
                    errors.append(r)
        done = min(i + batch_size, len(app_ids))
        if done % 200 == 0 or done == len(app_ids):
            print(f"  details: {done}/{len(app_ids)}")
        if done % 500 == 0 and done > 0:
            token = get_token()
        time.sleep(0.2)
    return all_details, errors, token


def update_talent_info(all_details, existing, token):
    new_ids = set()
    for app in all_details:
        reached = {st.get("stage_id") for st in app.get("stage_time_list", [])}
        if app.get("stage", {}).get("id"):
            reached.add(app["stage"]["id"])
        if reached & INTERVIEW_STAGES:
            tid = app.get("talent_id", "")
            if tid and tid not in existing:
                new_ids.add(tid)
    if new_ids:
        print(f"  new interview talents: {len(new_ids)}")
        for tid in new_ids:
            name = fetch_talent_name(tid, token)
            if name:
                existing[tid] = {"name": name, "experience_years": None, "city": ""}
            time.sleep(0.1)
    return existing


def build_data_js(all_details, talent_info):
    minimal_apps = []
    for app in all_details:
        reached = set()
        hr_enter = None
        stage_enter = [0] * 9  # stage index → enter_time (0 if never entered)
        for st in app.get("stage_time_list", []):
            sid = st.get("stage_id")
            idx = STAGE_INDEX.get(sid)
            if idx is not None:
                reached.add(idx)
                t = st.get("enter_time")
                if t:
                    stage_enter[idx] = int(t)
            if sid == "7484987042608285962":
                t = st.get("enter_time")
                if t:
                    hr_enter = int(t)
        current_sid = app.get("stage", {}).get("id")
        if current_sid:
            idx = STAGE_INDEX.get(current_sid)
            if idx is not None:
                reached.add(idx)
        current_idx = STAGE_INDEX.get(current_sid, -1)
        # Stage 0 (简历初筛) uses create_time if not otherwise recorded
        if stage_enter[0] == 0:
            stage_enter[0] = int(app.get("create_time", 0))
        record = [
            app.get("id", ""),
            app.get("talent_id", ""),
            int(app.get("create_time", 0)),
            app.get("active_status", 0),
            app.get("termination_type") or 0,
            current_idx,
            sorted(reached),
            hr_enter or 0,
            stage_enter,
        ]
        minimal_apps.append(record)

    talent_names = {tid: info["name"] for tid, info in talent_info.items() if info.get("name")}
    times = [a[2] for a in minimal_apps if a[2]]
    min_date = datetime.fromtimestamp(min(times) / 1000).strftime("%Y-%m-%d") if times else ""
    max_date = datetime.fromtimestamp(max(times) / 1000).strftime("%Y-%m-%d") if times else ""
    now = datetime.now()
    job_url = f"https://deepwisdom.feishu.cn/hire/job/{JOB_ID}"
    funnel_report_url = "https://deepwisdom.feishu.cn/hire/reports/custom/preview?lang=zh-CN&open_in_browser=true&key=7628440271964998588"
    links = {k: job_url for k in ["wiki", "writtenTestPipeline",
                                  "interview1_3m", "interview1_12m",
                                  "interview2_detail", "interview2_12m"]}
    links["hireReport"] = funnel_report_url
    output = {
        "dataPeriod": f"{min_date} \u2013 {max_date}",
        "updateTime": now.strftime("%Y-%m-%d %H:%M"),
        "dateRange": [min_date, max_date],
        "target": {"current": 3, "goal": 5, "gap": 2},
        "links": links,
        "talentNames": talent_names,
        "apps": minimal_apps,
    }
    return "var DASHBOARD_DATA = " + json.dumps(output, ensure_ascii=False, separators=(",", ":")) + ";\n"


def main():
    start = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] start refresh")
    token = get_token()
    print("  token OK")
    app_ids = list_application_ids(token)
    print(f"  applications: {len(app_ids)}")
    all_details, errors, token = batch_fetch_details(app_ids, token)
    print(f"  fetched: {len(all_details)} ok, {len(errors)} failed")

    talent_path = os.path.join(ROOT, "talent_info.json")
    talent_info = {}
    if os.path.exists(talent_path):
        with open(talent_path, "r", encoding="utf-8") as f:
            talent_info = json.load(f)
    talent_info = update_talent_info(all_details, talent_info, token)
    with open(talent_path, "w", encoding="utf-8") as f:
        json.dump(talent_info, f, ensure_ascii=False, indent=2)

    js = build_data_js(all_details, talent_info)
    with open(os.path.join(ROOT, "data.js"), "w", encoding="utf-8") as f:
        f.write(js)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] done in {time.time()-start:.1f}s · {len(all_details)} apps")


if __name__ == "__main__":
    main()
