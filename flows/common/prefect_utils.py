import os

def invalidate_cache(key: str) -> bool:
    cache_dir = os.getenv("PREFECT_CACHE_DIR")
    if not cache_dir:
        print("PREFECT_CACHE_DIR is not set, cannot invalidate cache.")
        return False
    path = os.path.join(cache_dir, key)
    if os.path.exists(path):
        os.remove(path)
        print(f"Cache file {path} removed.")
    else:
        print(f"Cache file {path} does not exist.")
    return True
