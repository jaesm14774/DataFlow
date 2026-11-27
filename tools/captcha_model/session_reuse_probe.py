"""Experiment (run ONLY when the site is not blocking): can one passed captcha serve several stocks?

If a session that already passed the captcha can query the next stock without a new captcha, collection gets several
times faster and sends far fewer requests. The script stops as soon as it has the answer and sends at most ~40 requests:

  1. keep trying (fresh session each time, with pauses) until one captcha is accepted for stock A
  2. in the SAME session try stock B in three ways and report whether the result page really contains stock B:
       a) POST again with the viewstate of the previous response and the same captcha text
       b) POST again with no captcha text at all
       c) GET bsContent directly after changing nothing (control: should still be stock A)

    docker exec -i dataflow-airflow-worker-1 python - 2330 2317 < tools/captcha_model/session_reuse_probe.py
"""
import os, sys, time, warnings
warnings.filterwarnings("ignore"); os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
sys.path.insert(0, "/opt/airflow/dags")
import numpy as np, requests, cv2, tensorflow as tf
from bs4 import BeautifulSoup
from stock.utilities_tf2 import preprocessing, one_hot_decoding
from stock.config.config import model_path

CODE_A, CODE_B = (sys.argv[1:3] + ["2330", "2317"])[:2]
B = "https://bsr.twse.com.tw/bshtm/"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
     "Referer": "https://bsr.twse.com.tw/"}
CHARS = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
FIELDS = ("__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS", "__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")
model = tf.keras.models.load_model(model_path)


def form(soup, code, captcha):
    data = {e.get("name"): (e.get("value") or "") for e in soup.find_all("input") if e.get("name") in FIELDS}
    data.update({"TextBox_Stkno": code, "btnOK": "查詢", "RadioButton_Normal": "RadioButton_Normal"})
    if captcha is not None:
        data["CaptchaControl1"] = captcha
    return data


def result_page(session):
    r = session.get(B + "bsContent.aspx?v=t", timeout=30, verify=False)
    soup = BeautifulSoup(r.text, "lxml")
    if r.status_code != 200 or soup.find("td", {"id": "receive_date"}) is None:
        return None
    text = soup.get_text(" ")
    return r.status_code, soup.find(class_="column_title_1") is not None, text


def passed_session(code):
    for attempt in range(1, 21):
        s = requests.Session()
        try:
            r = s.get(B + "bsMenu.aspx", headers=H, timeout=30, verify=False)
            soup = BeautifulSoup(r.text, "lxml")
            src = [x.get("src") for x in soup.find_all("img") if "CaptchaImage" in str(x)][0]
            img = s.get(B + src, timeout=30)
            if img.content[:2] != b"\xff\xd8":
                s.close(); time.sleep(2); continue
            open("/tmp/probe.jpg", "wb").write(img.content)
            preprocessing("/tmp/probe.jpg", "/tmp/probe_p.jpg")
            x = np.expand_dims(cv2.imread("/tmp/probe_p.jpg") / 255.0, 0)
            text = one_hot_decoding(model.predict(x, verbose=0), CHARS)
            post = s.post(B + "bsMenu.aspx", data=form(soup, code, text), headers=H, timeout=30, verify=False)
            page = result_page(s)
            print(f"attempt {attempt}: predicted {text} -> {'PASSED' if page else 'rejected'}")
            if page:
                return s, text, post
        except Exception as e:
            print("attempt", attempt, "error", type(e).__name__)
        s.close(); time.sleep(4)
    return None, None, None


s, text, post = passed_session(CODE_A)
if s is None:
    print("RESULT: could not pass a captcha in 20 attempts (site probably still blocking) - nothing to conclude")
    sys.exit(0)
print("stock A page contains", CODE_A, ":", CODE_A in result_page(s)[2][:3000])
post_soup = BeautifulSoup(post.text, "lxml")
for name, captcha in (("a) same captcha text, previous viewstate", text), ("b) no captcha text", None)):
    time.sleep(3)
    r = s.post(B + "bsMenu.aspx", data=form(post_soup if post_soup.find("input", {"name": "__VIEWSTATE"}) else BeautifulSoup(s.get(B + "bsMenu.aspx", headers=H, verify=False).text, "lxml"), CODE_B, captcha),
               headers=H, timeout=30, verify=False)
    page = result_page(s)
    if page is None:
        print(name, "-> result page not available (403): captcha is required for every stock")
    else:
        print(name, "-> result page OK, mentions", CODE_B, ":", CODE_B in page[2][:3000], "| has rows:", page[1])
print("RESULT: if (a) or (b) says 'mentions B: True', one captcha can serve several stocks (then rewrite collect() to reuse the session).")
