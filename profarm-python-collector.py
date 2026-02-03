import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import ambient
import paho.mqtt.client as mqtt
import requests

import config

# --- 定数・設定 ---
HOST = "pms.profarm-j.com"
USER_ID = config.USER_ID
PASSWORD = config.PASSWORD
SEL_HOUSE_ID = config.SEL_HOUSE_ID

# Beebotte設定 (config.pyに追記してください)
B_ACCESS_KEY = config.BEEBOTTE_ACCESS_KEY
B_SECRET_KEY = config.BEEBOTTE_SECRET_KEY
TOPIC = config.BEEBOTTE_TOPIC

# --- グローバル変数 ---
# ESP32からの外データを一時保存する箱
latest_outside_data = {"value": None, "timestamp": 0}

# 送信専用エグゼキューター（1スレッド制限）
executor = ThreadPoolExecutor(max_workers=1)


# --- MQTT コールバック関数 ---
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🤖 Beebotte接続成功")
        client.subscribe(TOPIC)
    else:
        print(f"Beebotte接続失敗: {reason_code}")


def on_message(client, userdata, msg):
    global latest_outside_data
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        val = payload.get("data")
        if val is not None:
            # 受信した値とMacの現在時刻を記録
            latest_outside_data = {"value": float(val), "timestamp": time.time()}
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 📥 Beebotte受信: {val} (トピック: {msg.topic})"
            )
        else:
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ Beebotte受信しましたが 'data' フィールドが空です: {payload}"
            )
    except Exception as e:
        print(
            f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ MQTT受信エラー: {e}"
        )


# --- 判定ロジック ---
def get_valid_outside_distance():
    """内部メモリをチェックし、10分以内なら値を返す"""
    global latest_outside_data
    val = latest_outside_data["value"]
    ts = latest_outside_data["timestamp"]

    if val is not None and (time.time() - ts < 600):
        return val
    return None


# --- 送信ワーカー関数 ---
def send_to_spreadsheet_worker(data_dict):
    """GASへ履歴データを送信"""
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
    except Exception as e:
        print(f"❌ SpreadSheet通信失敗: {e}")


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

    # 内部メモリから最新の外データを取得
    d4_val = get_valid_outside_distance()

    payload = {
        "created": dt_raw,
        "d1": to_num(data_dict.get("hom_Temp1")),
        "d2": to_num(data_dict.get("oum_AmountInso")),
        "d3": to_num(data_dict.get("nom_Sorinkling")),
    }

    # d4がNoneでない（有効な）時だけ追加する
    if d4_val is not None:
        payload["d4"] = d4_val
        print(
            f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🔗 合体成功: Houseデータ + 外距離({d4_val}) を送信します"
        )
    else:
        # データが古かった場合、その理由もわかると親切
        ts = latest_outside_data["timestamp"]
        diff = int(time.time() - ts) if ts > 0 else "なし"
        print(
            f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ⚠️ 外距離データが無効(経過:{diff}秒)のため、Houseデータのみ送信します"
        )

    try:
        res = am.send(payload)
        if res.status_code == 200:
            status_msg = f"d4={d4_val}" if d4_val else "d4=None(old/none)"
            print(
                f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🚀 Ambient送信完了 ({payload})"
            )
    except Exception as e:
        print(
            f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] ❌ Ambient通信エラー: {e}"
        )


# --- 送信指示（メインループから呼び出し） ---
def send_all(data_dict):
    executor.submit(send_to_spreadsheet_worker, data_dict)
    executor.submit(send_to_ambient_worker, data_dict)


def update_session_key(session, response_json):
    """レスポンスに含まれる新しいauth_keyでセッションを更新する"""
    new_key = response_json.get("auth_key")
    if new_key:
        session.cookies.set("data", new_key, domain=HOST)
        return True
    return False


# --- メイン処理 ---
def main():
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    needs_login = True
    last_send_status = last_history_data = last_alert_data = 0

    # MQTTクライアント初期化
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.username_pw_set(B_ACCESS_KEY, B_SECRET_KEY)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    print(f"[{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] 🚀 システム開始...")

    try:
        mqtt_client.connect("beebotte.com", 1883, 60)
        mqtt_client.loop_start()  # 別スレッドで受信開始
    except Exception as e:
        print(f"MQTT接続エラー: {e}")

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

                    # 合体送信実行
                    send_all(hist_data)

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
