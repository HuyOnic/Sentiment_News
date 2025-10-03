# load_fb_cookies_and_open_group.py
import pickle, time, os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from chung_khoan_viet_group import crawl_group, save_posts_csv

CHROME_DRIVER = os.getenv("CHROME_DRIVER", "/usr/local/bin/chromedriver")

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
    #     # Lưu kết quả
    posts = crawl_group(driver, 'https://www.facebook.com/groups/202428219869114', n_scrolls=3)
    save_posts_csv(posts, path="db_posts_1.csv")
    # Lưu kết quả
    # posts = crawl_group(driver, 'https://www.facebook.com/groups/congdongchungkhoanchinhthuc', n_scrolls=3)
    # save_posts_csv(posts, path="db_posts_2.csv")
finally:
    driver.quit()
