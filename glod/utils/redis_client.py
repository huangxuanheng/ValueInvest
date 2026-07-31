import redis
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Any


# Redis 连接配置
REDIS_HOST = "175.178.250.9"
REDIS_PORT = 6379
REDIS_PASSWORD = "123456"


class RedisClient:
    """Redis 客户端封装"""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._connect()
    
    def _connect(self):
        """建立 Redis 连接"""
        try:
            self._pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                max_connections=10,
            )
            self._client = redis.Redis(connection_pool=self._pool)
            # 测试连接
            self._client.ping()
            print(f'[Redis] 连接成功: {REDIS_HOST}:{REDIS_PORT}')
        except redis.ConnectionError as e:
            print(f'[Redis] 连接失败: {e}')
            self._client = None
        except Exception as e:
            print(f'[Redis] 初始化异常: {e}')
            self._client = None
    
    @property
    def client(self) -> Optional[redis.Redis]:
        """获取 Redis 客户端"""
        if self._client is None:
            self._connect()
        return self._client
    
    def is_available(self) -> bool:
        """检查 Redis 是否可用"""
        try:
            if self._client:
                self._client.ping()
                return True
        except Exception:
            pass
        return False
    
    def get_seconds_until_midnight(self) -> int:
        """计算距离今天0点的剩余秒数"""
        now = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return int((midnight - now).total_seconds())
    
    def set_cache(self, key: str, value: Any, expire_at_midnight: bool = True) -> bool:
        """
        设置缓存
        :param key: 缓存键
        :param value: 缓存值（自动序列化为JSON）
        :param expire_at_midnight: 是否在今天0点过期
        :return: 是否成功
        """
        try:
            client = self.client
            if not client:
                return False
            
            value_str = json.dumps(value, ensure_ascii=False)
            
            if expire_at_midnight:
                ttl = self.get_seconds_until_midnight()
                client.setex(key, ttl, value_str)
            else:
                client.set(key, value_str)
            
            return True
        except Exception as e:
            print(f'[Redis] 设置缓存失败 {key}: {e}')
            return False
    
    def get_cache(self, key: str) -> Optional[Any]:
        """
        获取缓存
        :param key: 缓存键
        :return: 缓存值（自动反序列化），不存在返回 None
        """
        try:
            client = self.client
            if not client:
                return None
            
            value = client.get(key)
            if value is None:
                return None
            
            return json.loads(value)
        except Exception as e:
            print(f'[Redis] 获取缓存失败 {key}: {e}')
            return None
    
    def delete_cache(self, key: str) -> bool:
        """删除缓存"""
        try:
            client = self.client
            if not client:
                return False
            client.delete(key)
            return True
        except Exception as e:
            print(f'[Redis] 删除缓存失败 {key}: {e}')
            return False
    
    def get_ttl(self, key: str) -> int:
        """获取缓存剩余时间（秒），-1表示永久，-2表示不存在"""
        try:
            client = self.client
            if not client:
                return -2
            return client.ttl(key)
        except Exception:
            return -2


# 全局实例
redis_client = RedisClient()


def get_cache_key(prefix: str, stock_code: str, **kwargs) -> str:
    """
    生成缓存键
    :param prefix: 前缀（如 'buffett:current_market'）
    :param stock_code: 股票代码
    :param kwargs: 其他参数
    :return: 缓存键
    """
    parts = [prefix, stock_code]
    for key, value in kwargs.items():
        if value is not None:
            parts.append(f"{key}={value}")
    return ":".join(parts)
