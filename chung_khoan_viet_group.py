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
    author = "Ẩn danh"
    try:
        header_candidates = article.find_element(
                                                By.CSS_SELECTOR,
                                                "span.html-span.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl.x1hl2dhg.x16tdsg8.x1vvkbs"
                                                )
        
        author = header_candidates.text.replace("\n","").strip()
        if len(author) > 20 or len(author)==0:
            author = "Ẩn danh"
        # print("Author:", author)
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
    except Exception:
        pass

    # --- Nội dung ---
    # Ưu tiên vùng data-ad-preview="message"; nếu không có, lấy khối text chính bên trong article
    content = ""
    try:
        # vùng message chính (nếu có)
        msg_blocks = article.find_elements(By.XPATH, ".//*[@data-ad-preview='message']//*[self::div or self::span or self::p]")
        if msg_blocks:
            parts = [get_text_safe(x) for x in msg_blocks]
            content = "\n".join([p for p in parts if p])

        if not content:
            # fallback: lấy toàn bộ text của article rồi lọc bớt các control
            content = get_text_safe(article)
        content = content.split("Tất cả cảm xúc:")[0]
        partern = r'(?:[a-zA-Z0-9ờ]\s*){20,}'
        content = re.sub(partern, '', content)
    except Exception:
        pass

    # Làm gọn
    if content:
        content = "\n".join([line.strip() for line in content.splitlines() if line.strip()])

    return {
        "author": author or "",
        "time": post_time or "",
        "content": content or "",
    }

def crawl_group(driver, group_url: str, n_scrolls: int = 50) -> List[Dict[str, str]]:
    wait = WebDriverWait(driver, 20)
    results = []
    seen_posts = set()
    prog_bar  = tqdm(range(n_scrolls))

    try:
        driver.get(group_url)
        print("✅ Đã vào được group:", group_url)

        # Chờ đến khi có bài đầu tiên
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[@role='article']")))

        last_height = driver.execute_script("return document.body.scrollHeight")

        for i in prog_bar:
            # print(f"🔽 Scroll lần {i + 1}")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(8, 10))

            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height <= last_height:
                print("⛔ Không còn nội dung mới, dừng scroll.")
                break
            last_height = new_height

            # Mở rộng các bài có nút "Xem thêm"
            try:
                see_more_buttons = driver.find_elements(By.XPATH, "//div[text()='Xem thêm']")
                for btn in see_more_buttons:
                    try:
                        driver.execute_script("arguments[0].click();", btn)
                    except Exception:
                        pass
            except NoSuchElementException:
                pass

            # articles = driver.find_elements(By.XPATH, "//div[@role='article']")
            articles = driver.find_elements(By.CSS_SELECTOR,"div.html-div.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu.xyri2b.x18d9i69.x1c1uobl")
            # print(f"🔍 Tìm thấy {len(articles)} bài viết")

            for art in articles:
                # print(art.get_attribute("outerHTML"))
                try:
                    data = extract_post(art)
                    content_id = f"{data['author']}|{data['time']}|{data['content'][:30]}"
                    if any([data["author"], data["time"], data["content"]]) and content_id not in seen_posts:
                        results.append(data)
                        seen_posts.add(content_id)
                except Exception as e:
                    print("⚠️ Lỗi khi xử lý bài viết:", e)

        print(f"✅ Tổng số bài viết thu thập được: {len(results)}")
        return results

    finally:
        driver.quit()


def save_posts_csv(posts, path="fb_posts.csv"):
    fieldnames = ["author", "time", "content"]

    df = pd.DataFrame(posts)
    print(df.columns)
    # Post processing 
    df = df[df["time"].notna()]
    df = df.drop_duplicates(subset=["content"])
    df = df[df["content"].str.len() > 15]
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Đã lưu {df.shape[0]} bản khi vào csv")

if __name__ == "__main__":
    posts = crawl_group(n_scrolls=8, min_posts=30)
    # In JSON để bạn dễ ghi file/process tiếp
    print(json.dumps(posts, ensure_ascii=False, indent=2))
