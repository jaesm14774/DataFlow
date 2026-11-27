"""Fetch ~24 live captcha images (GET only, no answer is ever submitted) and compare the production CNN's confidence
and simple pixel statistics with the old labelled set. Distribution shift would show up as lower confidence / different noise.
    docker exec -i dataflow-airflow-worker-1 python - < tools/captcha_model/live_vs_old.py
"""
import os, sys, time, warnings
warnings.filterwarnings("ignore"); os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
sys.path.insert(0, "/opt/airflow/dags")
import numpy as np, requests, cv2, tensorflow as tf
from bs4 import BeautifulSoup
tf.config.threading.set_intra_op_parallelism_threads(2)
from stock.utilities_tf2 import preprocessing
from stock.config.config import model_path
B = "https://bsr.twse.com.tw/bshtm/"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
os.makedirs("/tmp/live_imgs", exist_ok=True)
got, tries = 0, 0
while got < 24 and tries < 80:
    tries += 1
    try:
        with requests.Session() as s:
            r = s.get(B + "bsMenu.aspx", headers=H, timeout=30, verify=False)
            src = [x.get("src") for x in BeautifulSoup(r.text, "lxml").find_all("img") if "CaptchaImage" in str(x)][0]
            b = s.get(B + src, timeout=30)
            if b.content[:2] == b"\xff\xd8":
                open(f"/tmp/live_imgs/{got}.jpg", "wb").write(b.content); got += 1
    except Exception as e:
        print("EXC", type(e).__name__); time.sleep(5)
    time.sleep(2.5)
print("LIVE images", got, "from", tries, "requests")
def stats(paths):
    X = np.stack([preprocessing(p) / 255.0 for p in paths]).astype(np.float32)
    raw = np.stack([cv2.imread(p, 0) for p in paths]).astype(np.float32)
    m = tf.keras.models.load_model(model_path)
    pred = m.predict(X, verbose=0)
    conf = np.min([np.max(pred[k], axis=1) for k in range(5)], axis=0)
    return conf, raw.mean(), raw.std(), (raw > 200).mean(), (raw < 60).mean()
old = [f"/tmp/cap_data/captcha/{i}.jpg" for i in np.random.RandomState(0).choice(15000, 400, replace=False) + 1]
live = [f"/tmp/live_imgs/{i}.jpg" for i in range(got)]
for name, paths in (("OLD ", old), ("LIVE", live)):
    c, mean, std, bright, dark = stats(paths)
    print(name, "n=%d min-conf mean %.3f | frac<0.9: %.2f | frac<0.99: %.2f | gray mean %.1f std %.1f bright%% %.3f dark%% %.3f" %
          (len(paths), c.mean(), (c < 0.9).mean(), (c < 0.99).mean(), mean, std, bright, dark))
