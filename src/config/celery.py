from src.config.settings import settings

REDIS_URL = settings.redis_url

_rate_limit = settings.celery_rate_limit or None

CELERY_CONFIG = {
    "broker_url": REDIS_URL,
    "result_backend": REDIS_URL,

    "task_acks_late": True,             
    "worker_prefetch_multiplier": 1,    
    "task_track_started": True,
    "result_expires": settings.celery_result_expires,  

    "task_soft_time_limit": settings.worker_task_soft_time_limit,
    "task_time_limit": settings.worker_task_time_limit,

    "task_serializer": "json",
    "result_serializer": "json",
    "accept_content": ["json"],

    "worker_concurrency": settings.worker_concurrency,
    "worker_max_tasks_per_child": settings.worker_max_tasks_per_child,

    "task_annotations": {"*": {"rate_limit": _rate_limit}},

    "enable_utc": True,
}
