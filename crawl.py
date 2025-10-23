# load_fb_cookies_and_open_group.py
import pickle, time, os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from chung_khoan_viet_group import crawl_group, save_posts_csv
import pandas as pd
import datetime

CHROME_DRIVER = os.getenv("CHROME_DRIVER", "/usr/local/bin/chromedriver")
CHROME_DRIVER = "C:/Users/ADMIN/Downloads/chromedriver-win64/chromedriver.exe"
fb_sources = pd.read_csv("Sentiment_Source.csv")
groups = fb_sources[fb_sources["Loại nguồn"]=="Facebook"].loc[:, "Link"].tolist()

opts = Options()
# Có thể headless ở giai đoạn dùng lại cookie
# opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(CHROME_DRIVER), options=opts)

try:
    # PHẢI ở đúng domain trước khi add_cookie
    driver.get("https://www.facebook.com/")

    cookies = pickle.load(open("fb_cookies.pkl", "rb"))
    for c in cookies:
        # Chrom(e|ium) yêu cầu expiry kiểu int nếu có
        if "expiry" in c:
            c["expiry"] = int(c["expiry"])
        try:
            driver.add_cookie(c)
        except Exception:
            pass

    # Áp cookie
    driver.get("https://www.facebook.com/")

    posts = []
    print("Tổng số groups:", len(groups))

    for idx, group in enumerate(groups):
        print("Group:", idx)
        new_posts = crawl_group(driver, group, n_scrolls=8)
        if new_posts:
            posts += new_posts
    # Lưu kết quả
    now = datetime.datetime.now()
    formatted = now.strftime("%Y%m%d")
    save_posts_csv(posts, path=f"all_posts_{formatted}.csv")

finally:
    driver.quit()
