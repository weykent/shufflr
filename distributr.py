from uhashring import HashRing
import collections
import hashlib
import pendulum


def blake2b_hash(s):
    return hashlib.blake2b(s.encode()).digest()

def distribute(ids, posts_per_day=2.5):
    nodes = {}
    cur = pendulum.tomorrow()
    for _ in range(int(len(ids) * 24 / posts_per_day)):
        nodes[cur.isoformat()] = {'instance': cur, 'vnodes': 5}
        cur = cur.add(hours=1)
    ring = HashRing(nodes, hash_fn=blake2b_hash)
    for id in ids:
        hash = ring.hashi(str(id))
        yield id, ring[str(id)].add(minutes=hash[0])
