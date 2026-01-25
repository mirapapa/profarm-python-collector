import csv
import os
import time
from datetime import datetime

import requests

import config

HOST = "pms.profarm-j.com"
USER_ID = config.USER_ID
PASSWORD = config.PASSWORD
SEL_HOUSE_ID = config.SEL_HOUSE_ID
CSV_FILE = "profarm_data.csv"


def update_session_key(session, response_json):
    """レスポンスに含まれる新しいauth_keyでセッションを更新する"""
    new_key = response_json.get("auth_key")
    if new_key:
        session.cookies.set("data", new_key, domain=HOST)
        return True
    return False


def save_to_csv(data_dict):
    """履歴データをCSVに保存"""
    file_exists = os.path.isfile(CSV_FILE)
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

    row = {field: data_dict.get(field, "0") for field in fields}

    # ヒーター状態などの文字化け/空欄対策
    if "des_HeaterFireState" in row and (
        row["des_HeaterFireState"] == "0" or row["des_HeaterFireState"] is None
    ):
        row["des_HeaterFireState"] = "OFF"

    if not row.get("datadatetime"):
        row["datadatetime"] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ CSV保存失敗: {e}")


def main():
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    needs_login = True
    last_send_status = last_history_data = last_alert_data = 0

    print(
        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🚀 逐次キー更新モードで開始します..."
    )

    while True:
        now = time.time()
        current_ts = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        if needs_login:
            print(f"[{current_ts}] 🔐 ログイン実行中...")
            try:
                res = session.post(
                    f"https://{HOST}/login",
                    json={
                        "dispId": "ha0101u",
                        "lang": "ja",
                        "userId": USER_ID,
                        "password": PASSWORD,
                        "saveUserId": "0",
                    },
                )
                login_data = res.json()
                if update_session_key(session, login_data):
                    needs_login = False
                    print(f"[{current_ts}] ✅ ログイン成功")
                    last_send_status = last_history_data = last_alert_data = 0
                else:
                    print(f"[{current_ts}] ❌ ログイン失敗。5分待機。")
                    time.sleep(300)
                    continue
            except Exception as e:
                print(f"[{current_ts}] ❌ ログインエラー: {e}")
                time.sleep(300)
                continue

        try:
            # A. SEND_STATUS (5秒)
            # 📡 STATUS: {'auth_key': '.....', 'status': 200, 'alertCount': 0, 'progressvalue': '', 'progressstatus': ''}
            if now - last_send_status >= 5:
                res = session.post(
                    f"https://{HOST}/sendstatus",
                    json={
                        "selHouseId": SEL_HOUSE_ID,
                        "dispId": "hb0201u",
                        "lang": "ja",
                    },
                )
                st_json = res.json()
                # 再ログインが必要な場合
                if st_json.get("status") != 200:
                    print(
                        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 📡 STATUS: {st_json} ⚠️ 再ログインします。"
                    )
                    needs_login = True
                    time.sleep(60)
                    continue
                update_session_key(session, st_json)
                last_send_status = now

            # B. ALERT_DATA (60秒)
            # 🔔 ALERT: {'auth_key': '.....', 'status': 200, 'alertCount': 0, 'dim_FailSafe': '0'}
            if now - last_alert_data >= 60:
                res = session.post(
                    f"https://{HOST}/alertdata",
                    json={
                        "selHouseId": SEL_HOUSE_ID,
                        "dispId": "hb0201u",
                        "lang": "ja",
                    },
                )
                al_json = res.json()
                # 再ログインが必要な場合
                if al_json.get("status") != 200:
                    print(
                        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ 再ログインします。"
                    )
                    needs_login = True
                    time.sleep(60)
                    continue
                update_session_key(session, al_json)
                last_alert_data = now

            # C. HISTORY_DATA (60秒)
            if now - last_history_data >= 60:
                res = session.post(
                    f"https://{HOST}/historydata",
                    json={
                        "dispId": "hb0201u",
                        "lang": "ja",
                        "service": "get_historydata",
                    },
                )
                hist_data = res.json()
                # 履歴データ取得失敗時に再ログイン
                if hist_data.get("status") == 200:
                    update_session_key(session, hist_data)
                    save_to_csv(hist_data)
                    print(
                        f"[{hist_data.get('datadatetime')}] 📈 HISTORY: {hist_data.get('hom_Temp1')}℃"
                    )
                    last_history_data = now
                else:
                    print(
                        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ 履歴取得失敗。再ログインします。"
                    )
                    needs_login = True
                    time.sleep(60)
                    continue

        except Exception as e:
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ 通信エラー: {e}"
            )
            needs_login = True
            time.sleep(60)

        time.sleep(1)


if __name__ == "__main__":
    main()
