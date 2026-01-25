import csv
import os
import time
from datetime import datetime

import requests

# --- 設定ファイルを読み込む ---
import config

# 定数設定
HOST = "pms.profarm-j.com"
USER_ID = config.USER_ID
PASSWORD = config.PASSWORD
SEL_HOUSE_ID = config.SEL_HOUSE_ID
CSV_FILE = "profarm_data.csv"


def save_to_csv(data_dict):
    """取得した履歴データをCSVファイルに追記保存する"""
    file_exists = os.path.isfile(CSV_FILE)

    # 保存したい項目のリスト
    fields = [
        "datadatetime",
        "hom_Temp1",
        "hom_RelHumid1",
        "hom_SatDef1",
        "hom_Co2",
        "oum_Temp",
        "oum_AmountInso",
        "des_HeaterFireState",
    ]

    # データが存在しない場合のデフォルト値処理
    row = {}
    for field in fields:
        val = data_dict.get(field)
        if val is None:
            # 状態系ならOFF、数値系なら0をデフォルトにする
            row[field] = "OFF" if "State" in field else "0"
        else:
            row[field] = val

    # 日時がJSONにない場合は現在のシステム時刻を入れる
    if not row.get("datadatetime"):
        row["datadatetime"] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ CSV保存エラー: {e}")


def main():
    """メインループ"""
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    needs_login = True

    # 各リクエストの最終実行時刻を保持 (UNIXタイムスタンプ)
    last_send_status = 0
    last_history_data = 0
    last_alert_data = 0

    print(
        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🚀 モニタリングを開始します..."
    )
    print(f"設定: STATUS(5s), HISTORY(60s), ALERT(60s) / 保存先: {CSV_FILE}")

    while True:
        now = time.time()
        current_time_str = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        # --- 1. ログイン処理 (初回またはエラー発生時) ---
        if needs_login:
            print(f"[{current_time_str}] 🔐 ログイン処理を実行中...")
            try:
                login_res = session.post(
                    f"https://{HOST}/login",
                    json={
                        "dispId": "ha0101u",
                        "lang": "ja",
                        "userId": USER_ID,
                        "password": PASSWORD,
                        "saveUserId": "0",
                    },
                ).json()

                auth_key = login_res.get("auth_key")
                if auth_key:
                    session.cookies.set("data", auth_key, domain=HOST)
                    needs_login = False
                    print(
                        f"[{current_time_str}] ✅ ログイン成功 [Key: {auth_key[:8]}...]"
                    )
                    # ログイン直後に全データを取得するためタイマーをリセット
                    last_send_status = last_history_data = last_alert_data = 0
                else:
                    print(
                        f"[{current_time_str}] ❌ ログイン失敗。5分後にリトライします。 Response: {login_res}"
                    )
                    time.sleep(300)
                    continue
            except Exception as e:
                print(f"[{current_time_str}] ❌ ログインエラー: {e}")
                time.sleep(300)
                continue

        # --- 2. 各種データの取得実行 ---
        try:
            # A. SEND_STATUS (5秒おき)
            if now - last_send_status >= 5:
                res_status = session.post(
                    f"https://{HOST}/sendstatus",
                    json={
                        "selHouseId": SEL_HOUSE_ID,
                        "dispId": "hb0201u",
                        "lang": "ja",
                    },
                )
                st_json = res_status.json()
                print(
                    f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 📡 STATUS: {res_status.status_code} (Status:{st_json.get('status')})"
                )
                last_send_status = now

            # B. ALERT_DATA (60秒おき)
            if now - last_alert_data >= 60:
                res_alert = session.post(
                    f"https://{HOST}/alertdata",
                    json={
                        "selHouseId": SEL_HOUSE_ID,
                        "dispId": "hb0201u",
                        "lang": "ja",
                    },
                )
                print(
                    f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🔔 ALERT: {res_alert.json()}"
                )
                last_alert_data = now

            # C. HISTORY_DATA (60秒おき)
            if now - last_history_data >= 60:
                res_hist = session.post(
                    f"https://{HOST}/historydata",
                    json={
                        "dispId": "hb0201u",
                        "lang": "ja",
                        "service": "get_historydata",
                    },
                )
                data_hist = res_hist.json()

                if data_hist.get("status") == 200:
                    print(
                        f"[{data_hist.get('datadatetime')}] 📈 HISTORY取得完了 (Temp: {data_hist.get('hom_Temp1')}℃)"
                    )
                    save_to_csv(data_hist)
                    last_history_data = now
                else:
                    print(
                        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ 取得失敗 (Status: {data_hist.get('status')})。再ログインします。"
                    )
                    needs_login = True
                    time.sleep(300)
                    continue

        except Exception as e:
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ 通信エラー: {e}"
            )
            needs_login = True
            time.sleep(300)
            continue

        # CPU負荷軽減のための待機
        time.sleep(1)


if __name__ == "__main__":
    main()
