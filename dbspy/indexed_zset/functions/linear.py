from dbspy.indexed_zset import IndexedZSet, Indexer
from dbspy.zset import ZSet


def index_zset[I, T](zset: ZSet[T], indexer: Indexer[T, I]) -> IndexedZSet[I, T]:
    """Indexes a Z-Set according to some indexer function."""
    return IndexedZSet(zset.inner, indexer)