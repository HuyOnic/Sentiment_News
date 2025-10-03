from facebook_scraper import get_posts

for post in get_posts("techcombankjobs", pages=1):
    print(post)


