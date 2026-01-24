import csv
import os
import time
from datetime import datetime

import requests

# --- 設定ファイルを読み込む ---
import config

HOST = "pms.profarm-j.com"
USER_ID = config.USER_ID
PASSWORD = config.PASSWORD
SEL_HOUSE_ID = config.SEL_HOUSE_ID
INTERVAL = 60

CSV_FILE = "profarm_data.csv"


# データをCSVに保存する関数
def save_to_csv(data_dict):
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

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(data_dict)


# メインの処理
def main():
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    # 最初に「ログインが必要」な状態にしておく
    needs_login = True

    print(f"モニタリングを開始します...")

    while True:
        # 失敗フラグが立っている時だけログイン処理
        if needs_login:
            print(f"[{datetime.now()}] ログイン処理を実行中...")
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
                    print("✅ ログイン成功")
                else:
                    print("❌ ログイン失敗（auth_keyなし）。5分後にリトライします。")
                    time.sleep(300)
                    continue
            except Exception as e:
                print(f"❌ ログインエラー: {e}")
                time.sleep(300)
                continue

        # 履歴データの取得
        try:
            res = session.post(
                f"https://{HOST}/historydata",
                json={"dispId": "hb0201u", "lang": "ja", "service": "get_historydata"},
            )
            data = res.json()

            if data.get("status") == 200:
                print(
                    f"[{data.get('datadatetime')}] データ取得完了 (Temp: {data.get('hom_Temp1')}℃)"
                )
                save_to_csv(data)
            else:
                # 200以外（401など）が返ってきたら、次回ループで再ログインさせる
                print(
                    f"⚠️ 取得失敗（Status: {data.get('status')}）。次回ログインし直します。"
                )
                needs_login = True
                # 失敗時は少し待機して再開
                time.sleep(300)
                continue

        except Exception as e:
            print(f"通信エラー: {e}")
            # 通信エラーの場合も念のため再ログインフラグを立てる
            needs_login = True
            time.sleep(300)
            continue

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
