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

    def notify(self, msg, file_path=None):
        if file_path:
            with open(file_path, 'rb') as file:
                files = {
                    'payload_json': (None, json.dumps({"content": msg})),
                    'image.png': file,
                }
                response = requests.post(self.webhook_url, files=files, timeout=20)
        else:
            data = {"content": msg}
            response = requests.post(self.webhook_url, data=data, timeout=20)

        if response.status_code in [200, 201, 202, 203, 204]:
            print(f"{'File' if file_path else 'Message'} sent successfully.")
        else:
            print(f"Failed to send the {'file' if file_path else 'message'}.")
            print(response.text)
