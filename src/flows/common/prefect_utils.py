from datetime import datetime
import os

def maybe_invalidate_cache(data_date: datetime, cache_key: str, invalidate_seconds: int,
                           min_cache_age_seconds: int | None = None) -> bool:
    """
    Custom logic for invalidating the cache based on the data's timestamp.
    Scenario:
        result time  21:45 (12 minutes ago)
        cache time   21:53 (4 minutes ago)
        current time 21:57
    Cache is not outdated, but results are stale, so we invalidate the cache.
    Invalidation is done by removing the cache file.
    If min_cache_age_seconds is set, the cache is only invalidated when the
    cached result was fetched at least that long ago: re-fetching right after
    a fetch won't return newer data and only costs an API call.
    """
    date_now = datetime.now()
    print(f"Age of result: {date_now - data_date} seconds")
    if (date_now - data_date).total_seconds() > invalidate_seconds:
        # Invalidating the cache 12 minutes (not 10) after the result time,
        # as the data takes some time to be updated.
        if min_cache_age_seconds is not None:
            cache_age = get_cache_age_seconds(cache_key)
            if cache_age is None or cache_age < min_cache_age_seconds:
                age_text = "no cache file" if cache_age is None else f"{cache_age:.0f} seconds ago"
                print(f"Result is stale, but was fetched recently ({age_text}), not re-fetching.")
                return False
        print("Cache is outdated, invalidating...")
        return invalidate_cache(cache_key)
    return False

def get_cache_age_seconds(key: str) -> float | None:
    """Seconds since the cache file was written, or None if it does not exist."""
    cache_dir = os.getenv("PREFECT_CACHE_DIR")
    if not cache_dir:
        return None
    path = os.path.join(cache_dir, key)
    if not os.path.exists(path):
        return None
    return datetime.now().timestamp() - os.path.getmtime(path)

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
