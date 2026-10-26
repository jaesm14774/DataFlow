import requests
import json


class _AirflowCallbackMixin:
    """Airflow on_failure / on_success callback 的共用邏輯。
    子類別需實作 _send(msg, file_path=None)。
    """

    def _send(self, msg, file_path=None):
        raise NotImplementedError

    def _build_callback_msg(self, context, suffix):
        dag_run = context.get('dag_run')
        task_instance = dag_run.get_task_instances()[0]
        return (
            f'run_id : {task_instance.run_id}\n'
            f'dag_id : {task_instance.dag_id}\n'
            f'task_id : {task_instance.task_id}\n'
            f'operator : {task_instance.operator}\n'
            f'{suffix}'
        )

    def task_custom_failure_function(self, context):
        self._send(msg=self._build_callback_msg(context, 'failed !!!'))

    def task_custom_success_function(self, context):
        self._send(msg=self._build_callback_msg(context, 'success. ^^'))


class LineNotify(_AirflowCallbackMixin):
    token = ''

    def _send(self, msg, file_path=None):
        self.linenotify(msg, file_path)

    def linenotify(self, msg, file_path=None):
        headers = {"Authorization": "Bearer " + self.token}
        payload = {'message': '\n' + msg + '\n'}
        if file_path:
            with open(file_path, 'rb') as f:
                requests.post(
                    "https://notify-api.line.me/api/notify",
                    headers=headers, params=payload,
                    files={'imageFile': f}, timeout=20,
                )
        else:
            requests.post(
                "https://notify-api.line.me/api/notify",
                headers=headers, params=payload, timeout=20,
            )


class DiscordNotify(_AirflowCallbackMixin):
    webhook_url = ''

    def _send(self, msg, file_path=None):
        self.notify(msg, file_path)

    def notify(self, msg, file_path=None, embeds=None):
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
                response = requests.post(self.webhook_url, files=files, timeout=20)
        elif embeds:
            response = requests.post(self.webhook_url, json=payload, timeout=20)
        else:
            response = requests.post(self.webhook_url, data=payload, timeout=20)

        success = response.status_code in [200, 201, 202, 203, 204]
        if success:
            print(f"{'File' if file_path else 'Message'} sent successfully.")
        else:
            print(f"Failed to send the {'file' if file_path else 'message'}.")
            print(response.text)
        return success

    def notify_with_receipt(self, msg, file_path=None, embeds=None):
        """Send a Discord webhook and require the created message identifier."""
        payload = {"content": msg}
        if embeds:
            payload["embeds"] = embeds
            payload["allowed_mentions"] = {"parse": []}

        params = {"wait": "true"}
        try:
            if file_path:
                with open(file_path, 'rb') as file:
                    files = {
                        'payload_json': (None, json.dumps(payload, ensure_ascii=False)),
                        'image.png': file,
                    }
                    response = requests.post(
                        self.webhook_url, params=params, files=files, timeout=20,
                    )
            elif embeds:
                response = requests.post(
                    self.webhook_url, params=params, json=payload, timeout=20,
                )
            else:
                response = requests.post(
                    self.webhook_url, params=params, data=payload, timeout=20,
                )
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
