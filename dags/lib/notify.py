import requests
import json

#Line notify
class LineNotify(object):
    token=''
    
    def linenotify(self,msg,file_path=None):
        headers = {
            "Authorization": "Bearer " + self.token, 
        }
        payload = {'message': '\n'+msg+'\n'}
        if file_path:
            f={'imageFile':open(file_path,'rb')}
            r = requests.post("https://notify-api.line.me/api/notify", headers = headers, 
                        params = payload,
                        files=f, timeout=20)
        else:
            r = requests.post("https://notify-api.line.me/api/notify", headers = headers, 
                        params = payload, timeout=20)
    
    def task_custom_failure_function(self,context):
        dag_run = context.get('dag_run')
        task_instance = dag_run.get_task_instances()[0]
        
        msg=(
            f'run_id : {task_instance.run_id}\n'
            f'dag_id : {task_instance.dag_id}\n'
            f'task_id : {task_instance.task_id}\n'
            f'operator : {task_instance.operator}\n'
            f'failed !!!'
        )
        
        self.linenotify(msg=msg)
        
    def task_custom_success_function(self,context):
        dag_run = context.get('dag_run')
        task_instance = dag_run.get_task_instances()[0]
        
        msg=(
            f'run_id : {task_instance.run_id}\n'
            f'dag_id : {task_instance.dag_id}\n'
            f'task_id : {task_instance.task_id}\n'
            f'operator : {task_instance.operator}\n'
            f'success. ^^'
        )
        
        self.linenotify(msg=msg)

#discord notify
class DiscordNotify:
    webhook_url=''
    
    def notify(self, msg, file_path=None):
        if file_path:
            with open(file_path, 'rb') as file:
                files = {
                    'payload_json': (None, json.dumps({"content": msg})),
                    'image.png': file
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

    
    def task_custom_failure_function(self,context):
        dag_run = context.get('dag_run')
        task_instance = dag_run.get_task_instances()[0]
        
        msg=(
            f'run_id : {task_instance.run_id}\n'
            f'dag_id : {task_instance.dag_id}\n'
            f'task_id : {task_instance.task_id}\n'
            f'operator : {task_instance.operator}\n'
            f'failed !!!'
        )
        
        self.notify(msg=msg)
        
    def task_custom_success_function(self,context):
        dag_run = context.get('dag_run')
        task_instance = dag_run.get_task_instances()[0]
        
        msg=(
            f'run_id : {task_instance.run_id}\n'
            f'dag_id : {task_instance.dag_id}\n'
            f'task_id : {task_instance.task_id}\n'
            f'operator : {task_instance.operator}\n'
            f'success. ^^'
        )
        
        self.notify(msg=msg)