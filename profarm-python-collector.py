import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import ambient
import requests

import config

# --- 定数設定 ---
HOST = "pms.profarm-j.com"
USER_ID = config.USER_ID
PASSWORD = config.PASSWORD
SEL_HOUSE_ID = config.SEL_HOUSE_ID
CSV_FILE = "profarm_data.csv"

# Ambient設定
AMB_URL = f"http://ambidata.io/api/v2/channels/{config.AMBIENT_CHANNEL_ID}/data"
AMB_WRITE_KEY = config.AMBIENT_WRITE_KEY


# 1. 送信専用の窓口（エグゼキューター）を1つだけ作る
# これにより、同時に動く送信スレッドは必ず1つに制限されます
executor = ThreadPoolExecutor(max_workers=1)


def send_spreadsheet_worker(data_dict):
    """
    バックグラウンドでGASにデータを送信する（中身はそのまま）
    """
    fields = [
        "datadatetime",
        "hom_Temp1",
        "hom_Temp2",
        "hom_Temp24H1",
        "hom_Temp24H2",
        "hom_DifAveTemp1",
        "hom_RelHumid1",
        "hom_RelHumid2",
        "hom_SatDef1",
        "hom_SatDef2",
        "hom_Co2",
        "nom_Sorinkling",
        "oum_Temp",
        "oum_RelHumid",
        "oum_SatDef",
        "oum_WindSpeed",
        "oum_WindDir",
        "oum_AmountInso",
        "oum_AccumInso",
        "oum_RainFlg",
        "dem_SkylightURate1",
        "dem_SkylightURate2",
        "des_HeaterFireState",
        "des_HeaterBlowState",
        "des_Circulator1State",
        "des_Co2GeneratorState",
        "des_MistDeviceState",
        "des_SupplySignalState",
        "nom_CoolTemp",
    ]
    params = {f: data_dict.get(f, "") for f in fields}

    try:
        # timeoutはGASの処理時間を考慮して30秒に設定
        res = requests.get(config.GAS_URL, params=params, timeout=30)

        if res.status_code == 200:
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🟢 SpreadSheet送信完了: {res.text}"
            )
        else:
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🔴 SpreadSheetエラー: {res.status_code}"
            )
    except Exception as e:
        print(
            f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ SpreadSheet通信失敗: {e}"
        )


def send_to_spreadsheet(data_dict):
    """
    threading.Thread の代わりに executor.submit を使う
    """
    # 仕事をキュー（待ち行列）に追加する。
    # 前の仕事が終わっていなければ、終わるまで裏で待機してくれます。
    executor.submit(send_spreadsheet_worker, data_dict)


def get_house_distance():
    try:
        response = requests.get(f"{config.GAS_URL}?action=read", timeout=10)
        if response.status_code == 200:
            # "0.0,1706188000000" のような形式で届く
            parts = response.text.split(",")
            val = float(parts[0])
            last_update_ms = float(parts[1]) / 1000  # 秒単位に変換

            now_ts = time.time()
            # 600秒(10分)以上更新されていなければ「古い」と判断
            if now_ts - last_update_ms < 600:
                return val
            else:
                print(
                    f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ ハウスデータが古いためスキップします (最終更新: {datetime.fromtimestamp(last_update_ms)})"
                )
                return None  # 古い場合はNoneを返す
    except Exception as e:
        print(f"ハウスデータ取得エラー: {e}")
    return None


def send_to_ambient_worker(data_dict):
    """Ambient公式ライブラリを使って送信する"""
    # チャネルID(数値)とライトキー(文字列)で初期化
    am = ambient.Ambient(int(config.AMBIENT_CHANNEL_ID), config.AMBIENT_WRITE_KEY)

    # データの成形
    dt_raw = data_dict.get("datadatetime", "").replace("/", "-")

    # データを数値に変換（ライブラリを使う場合も数値型で渡すのが確実）
    def to_num(val):
        try:
            return float(val)
        except:
            return 0.0

    d4_val = get_house_distance()

    payload = {
        "created": dt_raw,
        "d1": to_num(data_dict.get("hom_Temp1")),
        "d2": to_num(data_dict.get("oum_AmountInso")),
        "d3": to_num(data_dict.get("nom_Sorinkling")),
    }

    # d4がNoneでない（有効な）時だけ追加する
    if d4_val is not None:
        payload["d4"] = d4_val

    try:
        res = am.send(payload)
        if res.status_code == 200:
            print(
                # f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🚀 Ambient送信成功: {payload['data'][0]}"
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🚀 Ambient送信成功: {payload}"
            )
        else:
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ Ambient送信失敗: {res.status_code}"
            )
    except Exception as e:
        print(
            f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ Ambient通信エラー: {e}"
        )


def send_to_ambient(data_dict):
    executor.submit(send_to_ambient_worker, data_dict)


def update_session_key(session, response_json):
    """レスポンスに含まれる新しいauth_keyでセッションを更新する"""
    new_key = response_json.get("auth_key")
    if new_key:
        session.cookies.set("data", new_key, domain=HOST)
        return True
    return False


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
                    print(
                        f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 📈 HISTORY({hist_data.get("datadatetime")}): {hist_data.get('hom_Temp1')}℃"
                    )
                    # 1. Ambient送信 (即時/ライブラリ)
                    send_to_ambient(hist_data)
                    # 2. スプレッドシート送信 (別スレッドで実行)
                    send_to_spreadsheet(hist_data)
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
