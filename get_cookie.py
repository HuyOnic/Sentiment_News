# save_fb_cookies.py
import os, pickle, sys, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
load_dotenv()

CHROME_DRIVER = os.getenv("CHROME_DRIVER", "/usr/local/bin/chromedriver")
FB_EMAIL = os.getenv("FB_EMAIL") or input("Nhập email FB: ").strip()
FB_PASS  = os.getenv("FB_PASS")  or input("Nhập password FB: ").strip()

opts = Options()
# KHÔNG headless khi login để tránh CAPTCHA
# opts.add_argument("--headless=new")
opts.add_argument("--disable-blink-features=AutomationControlled")
service = Service(CHROME_DRIVER)
driver = webdriver.Chrome(service=service, options=opts)

wait = WebDriverWait(driver, 60)

try:
    driver.get("https://www.facebook.com/login")

    # Chờ và điền email, password
    email_el = wait.until(EC.presence_of_element_located((By.ID, "email")))
    pass_el  = wait.until(EC.presence_of_element_located((By.ID, "pass")))
    email_el.clear(); email_el.send_keys(FB_EMAIL)
    pass_el.clear();  pass_el.send_keys(FB_PASS)

    # Click nút đăng nhập (name='login' ổn định nhất)
    login_btn = wait.until(EC.element_to_be_clickable((By.NAME, "login")))
    login_btn.click()

    # === Nếu có 2FA/CAPTCHA/Checkpoint ===
    # Cho bạn tối đa 5 phút để xác minh thủ công (nhập mã 2FA, solve captcha...)
    # Khi login thành công, cookie 'c_user' sẽ xuất hiện.
    WebDriverWait(driver, 300).until(lambda d: d.get_cookie("c_user") is not None)

    # Lưu cookie
    cookies = driver.get_cookies()
    with open("fb_cookies.pkl", "wb") as f:
        pickle.dump(cookies, f)
    print(f"✅ Đã lưu {len(cookies)} cookies vào fb_cookies.pkl")

except Exception as e:
    print("❌ Lỗi:", e)
    sys.exit(1)
finally:
    driver.quit()
