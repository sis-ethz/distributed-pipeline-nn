import hashlib


def hash_md5(url):
    return hashlib.md5(url.encode()).hexdigest()
