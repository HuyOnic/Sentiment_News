# crawl_fb_group_posts.py
import time, json
from typing import List, Dict, Optional
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import csv, os  # đảm bảo đã import ở đầu file
import random, re
from datetime import datetime, timedelta

months = {
    "Tháng 1": 1, "Tháng 2": 2, "Tháng 3": 3, "Tháng 4": 4,
    "Tháng 5": 5, "Tháng 6": 6, "Tháng 7": 7, "Tháng 8": 8,
    "Tháng 9": 9, "Tháng 10": 10, "Tháng 11": 11, "Tháng 12": 12
}

def get_text_safe(el) -> str:
    # Loại bỏ khoảng trắng
    try:
        return el.text.replace("\n"," ").strip()
    except Exception:
        return ""

def first_text_or_none(els) -> Optional[str]:
    for e in els:
        t = get_text_safe(e)
        if t:
            return t
    return None

def extract_post(article) -> Dict[str, str]:
    """Rút trích Người đăng, Thời gian, Nội dung từ 1 article."""
    # txt = article.text
    # print("Inner HTML", txt)
    # --- Người đăng ---
    # Tìm link tiêu đề trong header h2 (thường hiển thị display name)
    author = ""
    try:
        header_candidates = article.find_element(
                                                By.CSS_SELECTOR,
                                                "b.html-b.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl.x1hl2dhg.x16tdsg8.x1vvkbs.x1s688f"
                                                )
        
        author = header_candidates.text.replace("\n","").strip()
        if len(author) >= 30 or len(author)==0:
            return None
    except Exception as e:
        pass

    # --- Thời gian (timestamp/permalink) ---
    # Heuristic: link chứa '/posts/' hoặc element <time> có title/aria-label
    post_time = None
    try:
        # 1) permalink-like
        time_link = None
        for a in article.find_elements(By.XPATH, ".//a"):
            href = a.get_attribute("href") or ""
            if "/posts/" in href or "/permalink/" in href:
                time_link = a
                break

        if time_link:
            # Lấy tooltip/aria-label nếu có (thường ghi “14 minutes”, “Yesterday at ...”)
            post_time = time_link.get_attribute("aria-label") or get_text_safe(time_link)

        # 2) <time> node (nếu có)
        if not post_time:
            tnodes = article.find_elements(By.XPATH, ".//time")
            for t in tnodes:
                post_time = t.get_attribute("datetime") or t.get_attribute("title") or get_text_safe(t)
                if post_time:
                    break
        
        if not post_time:
            return None

        current_time = datetime.now()
        if "phút" in post_time:
            minutes = int(post_time.split(" ")[0])
            post_time = current_time - timedelta(minutes=minutes)
            post_time = post_time.strftime("%Y-%m-%d %H:%M:%S")

        elif "giờ" in post_time:
            hour = int(post_time.split(" ")[0])
            post_time = current_time - timedelta(hours=hour)
            post_time = post_time.strftime("%Y-%m-%d %H:%M:%S")

        elif "ngày" in post_time: 
            days = int(post_time.split(" ")[0])
            post_time = current_time - timedelta(days=days)
            post_time = post_time.strftime("%Y-%m-%d %H:%M:%S")

        elif "tuần" in post_time:
            weeks = int(post_time.split(" ")[0])
            post_time = current_time - timedelta(weeks=weeks)
            post_time = post_time.strftime("%Y-%m-%d %H:%M:%S")

        elif "năm" in post_time:
            years = int(post_time.split(" ")[0])
            post_time = current_time - timedelta(days=years*365)
            post_time = post_time.strftime("%Y-%m-%d %H:%M:%S")

        elif "https" in post_time:
            post_time = "2025-10-01 00:00:00"

        elif "Tháng" in post_time:
        # Tách dữ liệu
            parts = post_time.replace(",", "").split()
            day = int(parts[0])
            month = months[f"{parts[1]} {parts[2]}"]
            try:
                year = int(parts[3])
            except:
                year = 2025

            # Tạo datetime
            dt = datetime(year, month, day)

            # Chuyển định dạng
            post_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    
    except Exception:
        pass

    # --- Nội dung ---
    # Ưu tiên vùng data-ad-preview="message"; nếu không có, lấy khối text chính bên trong article
    content = ""
    try:
        # vùng message chính (nếu có)
        msg_block = article.find_element(By.CSS_SELECTOR, "div.html-div.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl")
        if msg_block:
            content = get_text_safe(msg_block)
        
        if not author:
            # fallback: lấy toàn bộ text của article rồi lọc bớt các control
            content = get_text_safe(article)
            real_author, real_content = split_by_capital_group(content)
            author = real_author
            content = real_content

        content = content.split("Thích Trả lời")[0].split("·")[-1].split("Tất cả cảm xúc")[0].strip()
        content = re.sub(r"\s*\d+\s*(phút|giờ|ngày|tuần|tháng|năm)\s*$", "", content)
        content_length = len(content.split(" "))
        
        if content_length<10: 
            return None

        if not author:
            return None
    except Exception:
        pass

    # Làm gọn
    if content:
        content = "\n".join([line.strip() for line in content.splitlines() if line.strip()])
        content = content.split("·")[-1]

    data = {
        "author": author or "",
        "time": post_time or "",
        "content": content or "",
    }    
    print(data)
    return data

def crawl_group(driver, group_url: str, n_scrolls: int = 50) -> List[Dict[str, str]]:
    wait = WebDriverWait(driver, 20)
    results = []
    seen_posts = set()
    prog_bar  = tqdm(range(n_scrolls))

    try:
        driver.get(group_url)
        print("Đã vào được group:", group_url)
        time.sleep(2)
        # Chờ đến khi có bài đầu tiên
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[@role='article']")))
        start_idx = 0

        for i in prog_bar:
            scroll_step = 1000*(i + 1)
            print(f"🔽 Scroll lần {i + 1}")

            driver.execute_script(f"window.scrollTo(0, {scroll_step});")
            time.sleep(random.uniform(1, 2))

            try:
                see_more_buttons = driver.find_elements(By.XPATH, "//div[text()='Xem thêm']")
                for btn in see_more_buttons:
                    try:
                        driver.execute_script("arguments[0].click();", btn)
                    except Exception:
                        pass
            except NoSuchElementException:
                pass

            # all_author_names = driver.find_elements(By.CSS_SELECTOR, "b.html-b.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl.x1hl2dhg.x16tdsg8.x1vvkbs.x1s688f") 
            # all_times = driver.find_elements(By.CSS_SELECTOR, "span.html-span.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl.x1hl2dhg.x16tdsg8.x1vvkbs")
            # print(len(all_author_names), "-", len(all_times))
            # for author_name, news_date in zip(all_author_names, all_times):
            #      print("Author:", author_name.text.replace("\n", ""), "- Time:", news_date.text)

            

            # articles = driver.find_elements(By.XPATH, "//div[@role='article']")
            articles = driver.find_elements(By.CSS_SELECTOR,"div.html-div.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl")
            print(f"🔍 Tìm thấy {len(articles[start_idx:])} bản ghi")
            
            for idx, art in enumerate(articles[start_idx:]):
                # print(art.get_attribute("outerHTML"))
                try:
                    data = extract_post(art)
                    if not data:
                        continue
                    content_id = f"{data['author']}|{data['time']}|{data['content'][:30]}"
                    if any([data["author"], data["time"], data["content"]]) and content_id not in seen_posts:
                        results.append(data)
                        seen_posts.add(content_id)
                        start_idx = idx
                except Exception as e:
                    print("⚠️ Lỗi khi xử lý bài viết:", e)

        print(f"✅ Tổng số bài viết thu thập được: {len(results)}")
        return results
    except Exception as e:
        print("Error:", e)

def split_by_capital_group(s: str):
    words = s.split()
    if not words:
        return "", ""

    # gom các từ viết hoa liên tiếp từ đầu
    idx = 0
    for w in words:
        if w[0].isupper():
            idx += 1
        else:
            break

    # nếu tất cả đều viết hoa thì substring2 rỗng
    substring1 = " ".join(words[:(idx-1)])
    substring2 = " ".join(words[(idx-1):])

    return substring1, substring2

def save_posts_csv(posts, path="fb_posts.csv"):
    fieldnames = ["author", "time", "content"]

    df = pd.DataFrame(posts)
    # Post processing 
    df = df[df["time"].notna()]
    df = df.drop_duplicates(subset=["content"])
    df = df[df["content"].str.len() > 20]
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu {df.shape[0]} bản khi vào csv")

if __name__ == "__main__":
    posts = crawl_group(n_scrolls=8, min_posts=30)
    # In JSON để bạn dễ ghi file/process tiếp
    print(json.dumps(posts, ensure_ascii=False, indent=2))
