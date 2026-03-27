#!/usr/bin/env python3
#
# CSE 434 Socket Programming Project - DHT Peer (Full Project)
# Group 39
# Firas Aljilaijil, Talall Alabadi
#
# python3 peer.py <manager_ip> <manager_port>

import socket
import sys
import os
import json
import csv
import threading
import time
import random

BUF = 65535


def is_prime(n):
    if n < 2: return False
    if n < 4: return True
    if n % 2 == 0 or n % 3 == 0: return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i+2) == 0: return False
        i += 6
    return True

def first_prime_above(x):
    c = x + 1
    while not is_prime(c):
        c += 1
    return c


class Peer:
    def __init__(self, mgr_ip, mgr_port):
        self.mgr_ip = mgr_ip
        self.mgr_port = mgr_port

        self.name = None
        self.ip = None
        self.m_port = None
        self.p_port = None
        self.m_sock = None
        self.p_sock = None

        # ring/dht
        self.my_id = None
        self.ring_size = None
        self.peer_tuples = []
        self.right_nbr = None
        self.local_ht = {}
        self.ht_size = 0
        self.is_leader = False
        self.year = None

        # ack tracking
        self.ack_count = 0
        self.ack_lock = threading.Lock()
        self.ack_event = threading.Event()

        # coordination events
        self.teardown_event = threading.Event()
        self.resetid_event = threading.Event()
        self.rebuild_done_event = threading.Event()
        self.rebuild_done_data = None

        self.listening = False

    # -- sockets --

    def setup_sockets(self):
        self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.m_sock.bind(('', self.m_port))
        self.p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.p_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.p_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB buffer
        self.p_sock.bind(('', self.p_port))
        self.p_sock.settimeout(0.5)
        print(f"Sockets ready: m_port={self.m_port} p_port={self.p_port}")

    def msg_manager(self, msg):
        self.m_sock.sendto(json.dumps(msg).encode(), (self.mgr_ip, self.mgr_port))
        print(f"  -> sent '{msg['command']}' to manager")

    def get_manager_reply(self):
        data, _ = self.m_sock.recvfrom(BUF)
        resp = json.loads(data.decode())
        print(f"  <- manager: {resp['command']} {resp['status']}")
        return resp

    def send_to(self, ip, port, msg):
        self.p_sock.sendto(json.dumps(msg).encode(), (ip, int(port)))

    def send_to_right(self, msg):
        if not self.right_nbr:
            print("ERROR: right_nbr not set!!")
            return
        self.send_to(self.right_nbr['ipv4_address'], self.right_nbr['p_port'], msg)

    def my_tuple(self):
        return {'peer_name': self.name, 'ipv4_address': self.ip, 'p_port': self.p_port}


    # -- p2p listener --

    def start_listener(self):
        if self.listening: return
        self.listening = True
        threading.Thread(target=self.p2p_loop, daemon=True).start()

    def p2p_loop(self):
        while self.listening:
            try:
                data, addr = self.p_sock.recvfrom(BUF)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode())
            except:
                continue

            cmd = msg.get('command', '')
            if cmd == 'set-id':          self.on_set_id(msg)
            elif cmd == 'set-id-ack':
                print(f"  got set-id-ack from {msg['peer_name']} (id={msg['id']})")
                with self.ack_lock: self.ack_count += 1
                self.ack_event.set()
            elif cmd == 'store':         self.on_store(msg)
            elif cmd == 'store-ack':
                with self.ack_lock: self.ack_count += 1
                self.ack_event.set()
            elif cmd == 'find-event':    self.on_find_event(msg)
            elif cmd == 'find-event-response': self.on_find_result(msg)
            elif cmd == 'teardown':      self.on_teardown(msg)
            elif cmd == 'reset-id':      self.on_reset_id(msg)
            elif cmd == 'rebuild-dht':
                threading.Thread(target=self.on_rebuild_dht, args=(msg,), daemon=True).start()
            elif cmd == 'rebuild-done':  self.on_rebuild_done(msg)
            elif cmd == 'join-request':
                threading.Thread(target=self.on_join_request, args=(msg,), daemon=True).start()


    # -- p2p handlers --

    def on_set_id(self, msg):
        self.my_id = msg['id']
        self.ring_size = msg['ring_size']
        self.peer_tuples = msg['peer_tuples']
        self.ht_size = msg.get('hash_table_size', 0)
        self.year = msg.get('year')
        self.right_nbr = self.peer_tuples[(self.my_id + 1) % self.ring_size]
        print(f"  got set-id: id={self.my_id}, ring={self.ring_size}, "
              f"right={self.right_nbr['peer_name']}")
        leader = self.peer_tuples[0]
        self.send_to(leader['ipv4_address'], leader['p_port'],
                    {'command': 'set-id-ack', 'peer_name': self.name, 'id': self.my_id})

    def on_store(self, msg):
        tid = msg['target_id']
        if tid == self.my_id:
            self.local_ht[msg['pos']] = msg['record']
            print(f"  Stored event {msg['record'].get('event_id','?')} at pos={msg['pos']} "
                  f"(local count={len(self.local_ht)})")
            leader = self.peer_tuples[0]
            self.send_to(leader['ipv4_address'], leader['p_port'],
                        {'command': 'store-ack', 'peer_name': self.name, 'pos': msg['pos']})
        else:
            self.send_to_right(msg)

    def on_find_event(self, msg):
        if msg.get('needs_hash') and self.ht_size > 0:
            eid = int(msg['event_id'])
            pos = eid % self.ht_size
            tid = pos % self.ring_size
            msg.update({'pos': pos, 'target_id': tid, 'needs_hash': False, 'id_seq': [self.my_id]})
            print(f"  computed hash: event {eid} -> pos={pos}, target={tid}")

        eid = int(msg['event_id'])
        sender = msg['sender']
        if msg['target_id'] == self.my_id:
            rec = self.local_ht.get(msg['pos'])
            if rec and int(rec['event_id']) == eid:
                resp = {'command': 'find-event-response', 'status': 'SUCCESS',
                        'event_id': eid, 'record': rec, 'id_seq': msg['id_seq']}
                print(f"  found event {eid}!")
            else:
                resp = {'command': 'find-event-response', 'status': 'FAILURE',
                        'event_id': eid, 'id_seq': msg['id_seq']}
                print(f"  event {eid} not at pos {msg['pos']}")
            self.send_to(sender['ipv4_address'], sender['p_port'], resp)
            return

        visited = set(msg['id_seq'])
        options = [i for i in range(self.ring_size) if i not in visited and i != msg['target_id']]
        if not options:
            if self.my_id not in msg['id_seq']: msg['id_seq'].append(self.my_id)
            dest = self.peer_tuples[msg['target_id']]
        else:
            nxt = random.choice(options)
            msg['id_seq'].append(nxt)
            dest = self.peer_tuples[nxt]
            print(f"  forwarding find-event to node {nxt} ({dest['peer_name']})")
        self.send_to(dest['ipv4_address'], dest['p_port'], msg)

    def on_find_result(self, msg):
        eid = msg['event_id']
        if msg['status'] == 'SUCCESS':
            print(f"\n{'='*50}")
            print(f"  FOUND event {eid}")
            print(f"  path: {msg['id_seq']}")
            print(f"{'='*50}")
            for k, v in msg['record'].items():
                print(f"  {k}: {v}")
            print(f"{'='*50}\n")
        else:
            print(f"\n  Storm event {eid} not found in the DHT.")
            print(f"  path: {msg['id_seq']}\n")

    def on_teardown(self, msg):
        if msg['originator'] == self.name:
            self.local_ht = {}
            self.ht_size = 0
            print(f"  teardown came back, cleared my HT")
            self.teardown_event.set()
        else:
            self.local_ht = {}
            self.ht_size = 0
            print(f"  teardown: cleared HT, forwarding")
            self.send_to_right(msg)

    def on_reset_id(self, msg):
        if msg['originator'] == self.name:
            print(f"  reset-id came back, all renumbered")
            self.resetid_event.set()
            return
        self.my_id = msg['new_id']
        self.ring_size = msg['new_ring_size']
        self.peer_tuples = msg['new_tuples']
        self.right_nbr = self.peer_tuples[(self.my_id + 1) % self.ring_size]
        print(f"  reset-id: id={self.my_id}, ring={self.ring_size}, right={self.right_nbr['peer_name']}")

        nxt_id = self.my_id + 1
        if nxt_id < self.ring_size:
            nxt = msg['new_tuples'][nxt_id]
            msg['new_id'] = nxt_id
            self.send_to(nxt['ipv4_address'], nxt['p_port'], msg)
        else:
            orig = msg['originator_tuple']
            self.send_to(orig['ipv4_address'], orig['p_port'], msg)

    def on_rebuild_dht(self, msg):
        # new leader rebuilds the DHT after a leave
        self.is_leader = True
        self.year = msg['year']
        requester = msg['requester']
        print(f"  rebuild-dht: I'm new leader, rebuilding year={self.year}")
        self._do_full_build()
        new_peers = [t['peer_name'] for t in self.peer_tuples]
        self.send_to(requester['ipv4_address'], requester['p_port'],
                    {'command': 'rebuild-done', 'new_leader': self.name, 'new_dht_peers': new_peers})
        print(f"  rebuild done, notified {requester['peer_name']}")

    def on_rebuild_done(self, msg):
        self.rebuild_done_data = msg
        self.rebuild_done_event.set()
        print(f"  got rebuild-done from {msg['new_leader']}")

    def on_join_request(self, msg):
        new_peer = msg['new_peer']
        print(f"  join-request from {new_peer['peer_name']}")

        # teardown existing DHT
        self.teardown_event.clear()
        self.send_to_right({'command': 'teardown', 'originator': self.name})
        self.teardown_event.wait(timeout=30)
        print(f"  teardown done for join")

        # add new peer to ring
        new_tuples = list(self.peer_tuples) + [new_peer]
        self.my_id = 0
        self.ring_size = len(new_tuples)
        self.peer_tuples = new_tuples
        self.right_nbr = new_tuples[1]

        # do a full build with new ring
        self._do_full_build()

        new_peers = [t['peer_name'] for t in new_tuples]
        self.send_to(new_peer['ipv4_address'], new_peer['p_port'],
                    {'command': 'rebuild-done', 'new_leader': self.name, 'new_dht_peers': new_peers})
        print(f"  join rebuild done, notified {new_peer['peer_name']}")


    # -- shared build logic: send set-ids, load csv, distribute stores --

    def _do_full_build(self):
        """Send set-ids, read CSV, distribute records. Assumes self.peer_tuples/ring_size/year set."""
        # load csv first so we know ht_size
        csv_path = self._find_csv()
        if not csv_path:
            print("  ERROR: csv not found!")
            return
        records = self.load_csv(csv_path)
        nrec = len(records)
        self.ht_size = first_prime_above(2 * nrec)
        print(f"  loaded {nrec} records, s={self.ht_size}")

        # send set-id to all peers with correct ht_size
        need_acks = self.ring_size - 1
        with self.ack_lock: self.ack_count = 0
        self.ack_event.clear()

        for i in range(1, self.ring_size):
            pt = self.peer_tuples[i]
            self.send_to(pt['ipv4_address'], pt['p_port'], {
                'command': 'set-id', 'id': i, 'ring_size': self.ring_size,
                'peer_tuples': self.peer_tuples,
                'hash_table_size': self.ht_size, 'year': self.year
            })

        deadline = time.time() + 30
        while self.ack_count < need_acks and time.time() < deadline:
            self.ack_event.wait(timeout=1.0)
            self.ack_event.clear()
        print(f"  {self.ack_count}/{need_acks} set-id acks")

        # distribute records
        with self.ack_lock: self.ack_count = 0
        self.ack_event.clear()

        remote = 0
        per_node = {i: 0 for i in range(self.ring_size)}
        for rec in records:
            eid = int(rec['event_id'])
            pos = eid % self.ht_size
            target = pos % self.ring_size
            per_node[target] += 1
            if target == 0:
                self.local_ht[pos] = rec
            else:
                self.send_to_right({'command': 'store', 'target_id': target,
                                   'pos': pos, 'record': rec})
                remote += 1
                if remote % 100 == 0: time.sleep(0.005)

        print(f"  distributed {nrec} records ({len(self.local_ht)} local, {remote} remote)")

        deadline = time.time() + 120
        while self.ack_count < remote and time.time() < deadline:
            self.ack_event.wait(timeout=1.0)
            self.ack_event.clear()

        if self.ack_count < remote:
            print(f"  WARNING: {self.ack_count}/{remote} store acks")
        else:
            print(f"  all {remote} store acks received")

        print(f"\n{'='*50}")
        print(f"  DHT Distribution (year={self.year}, n={self.ring_size}, s={self.ht_size})")
        print(f"{'='*50}")
        for i in range(self.ring_size):
            pn = self.peer_tuples[i]['peer_name']
            print(f"  Node {i} ({pn:15s}): {per_node[i]} records")
        total = sum(per_node.values())
        print(f"  Total: {total}")
        print(f"{'='*50}\n")

    def _find_csv(self):
        name = f"details-{self.year}.csv"
        for d in ['.', 'data', '../data', '/mnt/user-data/uploads']:
            p = os.path.join(d, name)
            if os.path.isfile(p): return p
        return None


    # -- stdin commands --

    def run(self):
        print(f"Peer started. Manager at {self.mgr_ip}:{self.mgr_port}")
        print("Commands: register, setup-dht, dht-complete, query-dht,")
        print("          leave-dht, join-dht, teardown-dht, deregister\n")
        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nexiting")
                break
            if not line: continue
            parts = line.split()
            cmd = parts[0].lower()
            if cmd == 'register':       self.do_register(parts)
            elif cmd == 'setup-dht':    self.do_setup_dht(parts)
            elif cmd == 'dht-complete': self.do_dht_complete(parts)
            elif cmd == 'query-dht':    self.do_query_dht(parts)
            elif cmd == 'teardown-dht': self.do_teardown_dht(parts)
            elif cmd == 'leave-dht':    self.do_leave_dht(parts)
            elif cmd == 'join-dht':     self.do_join_dht(parts)
            elif cmd == 'deregister':   self.do_deregister(parts)
            else: print(f"idk what '{cmd}' is")

    def do_register(self, parts):
        if len(parts) != 5:
            print("usage: register <n> <ip> <m_port> <p_port>"); return
        self.name, self.ip = parts[1], parts[2]
        self.m_port, self.p_port = int(parts[3]), int(parts[4])
        try: self.setup_sockets()
        except OSError as e: print(f"couldnt bind: {e}"); return
        self.start_listener()
        self.msg_manager({'command': 'register', 'peer_name': self.name,
                         'ipv4_address': self.ip, 'm_port': self.m_port, 'p_port': self.p_port})
        resp = self.get_manager_reply()
        if resp['status'] != 'SUCCESS': print(f"  failed: {resp.get('reason','')}")

    def do_setup_dht(self, parts):
        if len(parts) != 4:
            print("usage: setup-dht <n> <n> <year>"); return
        name, n, year = parts[1], int(parts[2]), parts[3]
        self.msg_manager({'command': 'setup-dht', 'peer_name': name, 'n': n, 'year': year})
        resp = self.get_manager_reply()
        if resp['status'] != 'SUCCESS':
            print(f"  setup failed: {resp.get('reason','')}"); return

        self.is_leader = True
        self.my_id = 0
        self.peer_tuples = resp['peer_tuples']
        self.ring_size = int(resp['n'])
        self.year = resp['year']
        self.right_nbr = self.peer_tuples[1 % self.ring_size]
        print(f"  I'm the leader (id=0), ring size={self.ring_size}")
        print(f"  right neighbour: {self.right_nbr['peer_name']}")

        self._do_full_build()
        print("  DHT setup done. Now send: dht-complete <your-name>")

    def do_dht_complete(self, parts):
        if len(parts) != 2: print("usage: dht-complete <n>"); return
        self.msg_manager({'command': 'dht-complete', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] == 'SUCCESS': print("  manager confirmed DHT is ready!")
        else: print(f"  failed: {resp.get('reason','')}")

    def do_query_dht(self, parts):
        if len(parts) != 2: print("usage: query-dht <n>"); return
        self.msg_manager({'command': 'query-dht', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] != 'SUCCESS':
            print(f"  query failed: {resp.get('reason','')}"); return
        target = resp['target']
        print(f"  routed to {target['peer_name']} ({target['ipv4_address']}:{target['p_port']})")
        try: eid_str = input("  enter event_id to look up: ").strip()
        except EOFError: return
        self.send_to(target['ipv4_address'], target['p_port'], {
            'command': 'find-event', 'event_id': int(eid_str),
            'pos': -1, 'target_id': -1, 'id_seq': [], 'needs_hash': True,
            'sender': self.my_tuple()
        })
        print(f"  sent find-event({eid_str}), waiting...")

    def do_teardown_dht(self, parts):
        if len(parts) != 2: print("usage: teardown-dht <n>"); return
        self.msg_manager({'command': 'teardown-dht', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] != 'SUCCESS':
            print(f"  failed: {resp.get('reason','')}"); return

        self.teardown_event.clear()
        self.send_to_right({'command': 'teardown', 'originator': self.name})
        print("  teardown sent around ring...")
        self.teardown_event.wait(timeout=30)
        print("  all HTs cleared")

        self.msg_manager({'command': 'teardown-complete', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] == 'SUCCESS':
            print("  manager confirmed teardown")
            self.is_leader = False
            self.my_id = None
            self.ring_size = None
            self.peer_tuples = []
            self.right_nbr = None
            self.year = None
        else:
            print(f"  teardown-complete failed: {resp.get('reason','')}")

    def do_leave_dht(self, parts):
        if len(parts) != 2: print("usage: leave-dht <n>"); return
        self.msg_manager({'command': 'leave-dht', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] != 'SUCCESS':
            print(f"  failed: {resp.get('reason','')}"); return
        print("  approved, starting leave process...")

        # teardown
        self.teardown_event.clear()
        self.send_to_right({'command': 'teardown', 'originator': self.name})
        self.teardown_event.wait(timeout=30)
        print("  teardown done")

        # build new ring without me, rotate so my right neighbour is leader
        old_right = self.right_nbr
        new_tuples = [t for t in self.peer_tuples if t['peer_name'] != self.name]
        # find old_right in new_tuples and rotate
        ri = 0
        for i, t in enumerate(new_tuples):
            if t['peer_name'] == old_right['peer_name']:
                ri = i; break
        new_tuples = new_tuples[ri:] + new_tuples[:ri]

        # reset-id
        self.resetid_event.clear()
        self.send_to(new_tuples[0]['ipv4_address'], new_tuples[0]['p_port'], {
            'command': 'reset-id', 'new_id': 0,
            'new_ring_size': len(new_tuples), 'new_tuples': new_tuples,
            'originator': self.name, 'originator_tuple': self.my_tuple()
        })
        print(f"  reset-id sent, new leader = {new_tuples[0]['peer_name']}")
        self.resetid_event.wait(timeout=30)
        print("  all renumbered")

        # rebuild
        self.rebuild_done_event.clear()
        self.rebuild_done_data = None
        self.send_to(new_tuples[0]['ipv4_address'], new_tuples[0]['p_port'], {
            'command': 'rebuild-dht', 'year': self.year, 'requester': self.my_tuple()
        })
        print("  rebuild-dht sent, waiting...")
        self.rebuild_done_event.wait(timeout=180)

        data = self.rebuild_done_data
        new_leader = data['new_leader'] if data else new_tuples[0]['peer_name']
        new_peers = data['new_dht_peers'] if data else [t['peer_name'] for t in new_tuples]

        self.msg_manager({'command': 'dht-rebuilt', 'peer_name': self.name,
                         'new_leader': new_leader, 'new_dht_peers': new_peers})
        resp = self.get_manager_reply()
        if resp['status'] == 'SUCCESS':
            print("  I'm now Free")
            self.is_leader = False
            self.my_id = None
            self.ring_size = None
            self.peer_tuples = []
            self.right_nbr = None
            self.local_ht = {}
        else:
            print(f"  dht-rebuilt failed: {resp.get('reason','')}")

    def do_join_dht(self, parts):
        if len(parts) != 2: print("usage: join-dht <n>"); return
        self.msg_manager({'command': 'join-dht', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] != 'SUCCESS':
            print(f"  failed: {resp.get('reason','')}"); return

        leader = resp['leader']
        self.year = resp.get('year')
        print(f"  approved, leader is {leader['peer_name']}")

        self.rebuild_done_event.clear()
        self.rebuild_done_data = None
        self.send_to(leader['ipv4_address'], leader['p_port'],
                    {'command': 'join-request', 'new_peer': self.my_tuple()})
        print("  join-request sent, waiting for rebuild...")
        self.rebuild_done_event.wait(timeout=180)

        data = self.rebuild_done_data
        if not data:
            print("  ERROR: rebuild timed out!"); return
        print(f"  rebuild done! I'm in the DHT now")

        self.msg_manager({'command': 'dht-rebuilt', 'peer_name': self.name,
                         'new_leader': data['new_leader'], 'new_dht_peers': data['new_dht_peers']})
        resp = self.get_manager_reply()
        if resp['status'] == 'SUCCESS':
            print("  manager confirmed, DHT rebuilt")
        else:
            print(f"  dht-rebuilt failed: {resp.get('reason','')}")

    def do_deregister(self, parts):
        if len(parts) != 2: print("usage: deregister <n>"); return
        self.msg_manager({'command': 'deregister', 'peer_name': parts[1]})
        resp = self.get_manager_reply()
        if resp['status'] == 'SUCCESS':
            print("  deregistered, bye")
            self.listening = False
            sys.exit(0)
        else:
            print(f"  nope: {resp.get('reason','')}")

    def load_csv(self, path):
        rows = []
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                clean = {}
                for k, v in row.items():
                    clean[k.strip().lower().replace(' ', '_')] = v.strip() if v else ''
                if 'event_id' not in clean or clean['event_id'] == '': continue
                rows.append(clean)
        return rows


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f"Usage: python3 {sys.argv[0]} <manager_ip> <manager_port>")
        sys.exit(1)
    Peer(sys.argv[1], int(sys.argv[2])).run()
