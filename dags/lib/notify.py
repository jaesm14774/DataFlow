import json

import pandas as pd
import requests


class _AirflowCallbackMixin:
    """Airflow on_failure callback 的共用邏輯。
    子類別需實作 _send(msg, file_path=None)。
    """

    def _send(self, msg, file_path=None):
        raise NotImplementedError

    def _build_callback_msg(self, context, suffix):
        # 失敗的是 context 裡的 task_instance；get_task_instances()[0] 會回傳 DAG 的第一個 task，不一定是失敗的那個
        task_instance = context.get('task_instance') or context['dag_run'].get_task_instances()[0]
        return (
            f'run_id : {task_instance.run_id}\n'
            f'dag_id : {task_instance.dag_id}\n'
            f'task_id : {task_instance.task_id}\n'
            f'operator : {task_instance.operator}\n'
            f'{suffix}'
        )

    def task_custom_failure_function(self, context):
        self._send(msg=self._build_callback_msg(context, 'failed !!!'))


class DiscordNotify(_AirflowCallbackMixin):
    webhook_url = ''

    def _send(self, msg, file_path=None):
        self.notify(msg, file_path)

    def _post(self, msg, file_path=None, embeds=None, params=None):
        """送出 webhook 請求並回傳 requests.Response（網路例外由呼叫端處理）。"""
        payload = {"content": msg}
        if embeds:
            payload["embeds"] = embeds
            payload["allowed_mentions"] = {"parse": []}

        if file_path:
            with open(file_path, 'rb') as file:
                files = {
                    'payload_json': (None, json.dumps(payload, ensure_ascii=False)),
                    'image.png': file,
                }
                return requests.post(
                    self.webhook_url, params=params, files=files, timeout=20,
                )
        if embeds:
            return requests.post(
                self.webhook_url, params=params, json=payload, timeout=20,
            )
        return requests.post(
            self.webhook_url, params=params, data=payload, timeout=20,
        )

    def notify(self, msg, file_path=None, embeds=None):
        response = self._post(msg, file_path, embeds)

        success = response.status_code in [200, 201, 202, 203, 204]
        if success:
            print(f"{'File' if file_path else 'Message'} sent successfully.")
        else:
            print(f"Failed to send the {'file' if file_path else 'message'}.")
            print(response.text)
        return success

    def notify_with_receipt(self, msg, file_path=None, embeds=None):
        """Send a Discord webhook and require the created message identifier."""
        try:
            response = self._post(msg, file_path, embeds, params={"wait": "true"})
        except requests.RequestException as exc:
            print(
                "Discord webhook request outcome is unknown after "
                f"{type(exc).__name__}."
            )
            return {
                "accepted": None,
                "success": False,
                "http_status": None,
                "receipt_id": None,
                "outcome_unknown": True,
                "error_type": type(exc).__name__,
            }

        accepted = response.status_code in [200, 201, 202, 203, 204]
        receipt_id = None
        if accepted:
            try:
                response_payload = response.json()
            except ValueError:
                response_payload = None
            if isinstance(response_payload, dict):
                raw_receipt_id = response_payload.get("id")
                normalized_receipt_id = str(raw_receipt_id or "").strip()
                if normalized_receipt_id.isdigit():
                    receipt_id = normalized_receipt_id

        success = accepted and receipt_id is not None
        if success:
            print("Discord message sent with a platform receipt.")
        elif accepted:
            print("Discord accepted the request but returned no message receipt ID.")
        else:
            print(f"Discord webhook request failed with HTTP {response.status_code}.")
        return {
            "accepted": accepted,
            "success": success,
            "http_status": int(response.status_code),
            "receipt_id": receipt_id,
            "outcome_unknown": response.status_code >= 500,
            "error_type": None,
        }


def discord_notifier(label, token_path=None):
    """依頻道名稱(label)從 Discord token 檔建立 DiscordNotify。

    token_path:未指定時使用 common.config 的預設值；
    stock 模組使用 stock/config 底下的另一份 token 檔，需明確傳入。
    """
    if token_path is None:
        from common.config import discord_token_path as token_path
    tokens = pd.read_csv(token_path, encoding='utf_8_sig', index_col='name')
    notifier = DiscordNotify()
    notifier.webhook_url = tokens.loc[label, 'token']
    return notifier
