"""Evaluate the production captcha CNN on the labelled 15,000 images (read-only; no network).

Run inside the Airflow worker (has TensorFlow 2.16 + the same preprocessing as production):
    docker exec -i dataflow-airflow-worker-1 python - < tools/captcha_model/eval_baseline.py
"""
import os, sys, warnings
warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
sys.path.insert(0, "/opt/airflow/dags")
import numpy as np, pandas as pd, cv2, tensorflow as tf
tf.config.threading.set_intra_op_parallelism_threads(4)
tf.config.threading.set_inter_op_parallelism_threads(2)
from stock.utilities_tf2 import preprocessing
from stock.config.config import model_path

CHARS = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
DATA = "/tmp/cap_data"
labels = pd.read_csv(f"{DATA}/label.csv", header=None)[0].tolist()
n = len(labels)
X = np.zeros((n, 60, 200, 3), np.float32)
for i in range(n):
    X[i] = preprocessing(f"{DATA}/captcha/{i + 1}.jpg") / 255.0
model = tf.keras.models.load_model(model_path)
pred = model.predict(X, batch_size=256, verbose=0)
text = ["".join(CHARS[int(np.argmax(pred[k][i]))] for k in range(5)) for i in range(n)]
ok = np.array([a == b for a, b in zip(text, labels)])
per_char = np.mean([[a[k] == b[k] for k in range(5)] for a, b in zip(text, labels)], axis=0)
split = int(n * 0.8)
print("ALL    full-captcha acc %.3f" % ok.mean())
print("TRAIN  (first 80%%) acc %.3f" % ok[:split].mean())
print("HOLDOUT(last 20%%, Keras validation_split) acc %.3f" % ok[split:].mean())
print("per-position char acc", np.round(per_char, 3))
conf = np.min([np.max(pred[k], axis=1) for k in range(5)], axis=0)
print("mean min-confidence correct %.3f wrong %.3f" % (conf[ok].mean(), conf[~ok].mean() if (~ok).any() else float("nan")))
