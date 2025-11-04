from multiprocessing import Pool, cpu_count
import pandas as pd
import pickle, os
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from chung_khoan_viet_group import crawl_group, save_posts_csv

CHROME_DRIVER = "C:/Users/ADMIN/Downloads/chromedriver-win64/chromedriver.exe"
fb_sources = pd.read_csv("Sentiment_Source.csv")
groups = fb_sources[fb_sources["Loại nguồn"]=="Facebook"]["Link"].tolist()

def worker(group_batch):
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # opts.add_argument("--headless=new")  # tùy chọn

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER), options=opts)
    driver.get("https://www.facebook.com/")

    # Load cookies
    cookies = pickle.load(open("fb_cookies.pkl", "rb"))
    for c in cookies:
        if "expiry" in c:
            c["expiry"] = int(c["expiry"])
        try:
            driver.add_cookie(c)
        except:
            pass

    driver.get("https://www.facebook.com/")

    all_posts = []
    for g in group_batch:
        print(f"[PID {os.getpid()}] Crawling: {g}")
        try:
            posts = crawl_group(driver, g, n_scrolls=20)
            if posts:
                all_posts.extend(posts)
        except Exception as e:
            print("Lỗi:", e)

    driver.quit()
    return all_posts

if __name__ == '__main__':
    n_proc = min(8, cpu_count())  # tối đa 4 tab Chrome song song là hợp lý
    chunk = len(groups) // n_proc + 1
    batches = [groups[i:i + chunk] for i in range(0, len(groups), chunk)]

    with Pool(n_proc) as p:
        result = p.map(worker, batches)

    # Gộp kết quả
    final_posts = [x for batch in result for x in batch]

    formatted = datetime.datetime.now().strftime("%Y%m%d")
    output_path = f"all_posts_{formatted}.csv"
    save_posts_csv(final_posts, output_path)
    print("✅ DONE:", len(final_posts), "to", output_path)
