#!/usr/bin/env python3
#
# CSE 434 Socket Programming Project - Manager (Full Project)
# Group 39
# Firas Aljilaijil, Talall Alabadi
#

import socket
import sys
import json
import random

BUF = 65535

class Manager:
    def __init__(self, port):
        self.port = port
        self.peers = {}
        self.dht_exists = False
        self.dht_leader = None
        self.dht_size = 0
        self.dht_year = None
        self.dht_peers = []
        self.waiting_for = None
        self.churn_peer = None
        self.churn_action = None  # 'leave' or 'join'

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', self.port))
        print(f"Manager listening on port {self.port}")

    def reply(self, addr, command, status, extra=None):
        resp = {'command': command, 'status': status}
        if extra:
            resp.update(extra)
        self.sock.sendto(json.dumps(resp).encode(), addr)
        print(f"   -> {status} to {addr}")

    def show_peers(self):
        print("   [Peer Table]")
        if len(self.peers) == 0:
            print("   (empty)")
            return
        for n, i in self.peers.items():
            print(f"   {n:15s} {i['ip']:15s} m={i['m_port']} p={i['p_port']} state={i['state']} id={i['id']}")

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(BUF)
            try:
                msg = json.loads(data.decode())
            except:
                print(f"bad packet from {addr}")
                continue

            cmd = msg.get('command', '')
            print(f"\n>> {cmd} from {addr}")

            if self.waiting_for and cmd != self.waiting_for:
                print(f"   blocked, need {self.waiting_for} first")
                self.reply(addr, cmd, 'FAILURE',
                           {'reason': f"manager is waiting for {self.waiting_for}"})
                continue

            if cmd == 'register':           self.handle_register(msg, addr)
            elif cmd == 'setup-dht':        self.handle_setup_dht(msg, addr)
            elif cmd == 'dht-complete':     self.handle_dht_complete(msg, addr)
            elif cmd == 'query-dht':        self.handle_query_dht(msg, addr)
            elif cmd == 'leave-dht':        self.handle_leave(msg, addr)
            elif cmd == 'join-dht':         self.handle_join(msg, addr)
            elif cmd == 'dht-rebuilt':      self.handle_rebuilt(msg, addr)
            elif cmd == 'deregister':       self.handle_deregister(msg, addr)
            elif cmd == 'teardown-dht':     self.handle_teardown(msg, addr)
            elif cmd == 'teardown-complete': self.handle_teardown_done(msg, addr)
            else:
                print(f"   dont know what '{cmd}' is")
                self.reply(addr, cmd, 'FAILURE', {'reason': 'unknown command'})


    # --- REGISTER ---
    def handle_register(self, msg, addr):
        name = msg['peer_name']
        ip = msg['ipv4_address']
        mp = int(msg['m_port'])
        pp = int(msg['p_port'])

        if name in self.peers:
            self.reply(addr, 'register', 'FAILURE', {'reason': 'Name already taken'})
            return
        if mp == pp:
            self.reply(addr, 'register', 'FAILURE', {'reason': 'm_port and p_port cant be same'})
            return

        for pn, pi in self.peers.items():
            if pi['ip'] == ip:
                if mp in (pi['m_port'], pi['p_port']) or pp in (pi['m_port'], pi['p_port']):
                    self.reply(addr, 'register', 'FAILURE',
                               {'reason': f'port conflict with {pn}'})
                    return

        self.peers[name] = {
            'ip': ip, 'm_port': mp, 'p_port': pp,
            'state': 'Free', 'id': None
        }
        print(f"   registered {name} ({ip}:{mp}/{pp})")
        self.show_peers()
        self.reply(addr, 'register', 'SUCCESS')


    # --- SETUP-DHT ---
    def handle_setup_dht(self, msg, addr):
        name = msg['peer_name']
        n = int(msg['n'])
        year = msg['year']

        if name not in self.peers:
            self.reply(addr, 'setup-dht', 'FAILURE', {'reason': 'not registered'})
            return
        if self.peers[name]['state'] != 'Free':
            self.reply(addr, 'setup-dht', 'FAILURE', {'reason': 'peer not Free'})
            return
        if n < 3:
            self.reply(addr, 'setup-dht', 'FAILURE', {'reason': 'n must be >= 3'})
            return
        if self.dht_exists:
            self.reply(addr, 'setup-dht', 'FAILURE', {'reason': 'DHT already exists'})
            return

        free_peers = [p for p in self.peers if self.peers[p]['state'] == 'Free']
        if len(free_peers) < n:
            self.reply(addr, 'setup-dht', 'FAILURE',
                       {'reason': f'need {n} peers but only {len(free_peers)} are free'})
            return

        others = [p for p in free_peers if p != name]
        picked = random.sample(others, n - 1)

        self.peers[name]['state'] = 'Leader'
        self.peers[name]['id'] = 0
        for idx, pname in enumerate(picked, 1):
            self.peers[pname]['state'] = 'InDHT'
            self.peers[pname]['id'] = idx

        self.dht_exists = True
        self.dht_leader = name
        self.dht_size = n
        self.dht_year = year
        self.dht_peers = [name] + picked

        tuples = []
        for pname in self.dht_peers:
            p = self.peers[pname]
            tuples.append({
                'peer_name': pname,
                'ipv4_address': p['ip'],
                'p_port': p['p_port']
            })

        print(f"   dht created: leader={name}, n={n}, year={year}")
        print(f"   ring: {[t['peer_name'] for t in tuples]}")

        self.waiting_for = 'dht-complete'
        self.reply(addr, 'setup-dht', 'SUCCESS',
                   {'peer_tuples': tuples, 'n': n, 'year': year})


    def handle_dht_complete(self, msg, addr):
        name = msg['peer_name']
        if name != self.dht_leader:
            self.reply(addr, 'dht-complete', 'FAILURE', {'reason': 'not the leader'})
            return
        self.waiting_for = None
        print(f"   dht complete! leader={name}")
        self.show_peers()
        self.reply(addr, 'dht-complete', 'SUCCESS')


    def handle_query_dht(self, msg, addr):
        name = msg['peer_name']
        if not self.dht_exists:
            self.reply(addr, 'query-dht', 'FAILURE', {'reason': 'no DHT active'})
            return
        if name not in self.peers:
            self.reply(addr, 'query-dht', 'FAILURE', {'reason': 'not registered'})
            return
        if self.peers[name]['state'] != 'Free':
            self.reply(addr, 'query-dht', 'FAILURE', {'reason': 'peer must be Free'})
            return
        pick = random.choice(self.dht_peers)
        info = self.peers[pick]
        t = {'peer_name': pick, 'ipv4_address': info['ip'], 'p_port': info['p_port']}
        print(f"   routing {name}'s query to {pick}")
        self.reply(addr, 'query-dht', 'SUCCESS', {'target': t})


    # --- LEAVE-DHT ---
    def handle_leave(self, msg, addr):
        name = msg['peer_name']
        if not self.dht_exists:
            self.reply(addr, 'leave-dht', 'FAILURE', {'reason': 'no DHT'})
            return
        if name not in self.peers or self.peers[name]['state'] not in ('Leader', 'InDHT'):
            self.reply(addr, 'leave-dht', 'FAILURE', {'reason': 'not in DHT'})
            return
        if self.dht_size <= 3:
            self.reply(addr, 'leave-dht', 'FAILURE', {'reason': 'DHT too small to lose a peer'})
            return
        self.churn_peer = name
        self.churn_action = 'leave'
        self.waiting_for = 'dht-rebuilt'
        print(f"   {name} leaving dht")
        self.reply(addr, 'leave-dht', 'SUCCESS')

    # --- JOIN-DHT ---
    def handle_join(self, msg, addr):
        name = msg['peer_name']
        if not self.dht_exists:
            self.reply(addr, 'join-dht', 'FAILURE', {'reason': 'no DHT'})
            return
        if name not in self.peers or self.peers[name]['state'] != 'Free':
            self.reply(addr, 'join-dht', 'FAILURE', {'reason': 'peer not Free'})
            return

        self.churn_peer = name
        self.churn_action = 'join'
        self.waiting_for = 'dht-rebuilt'

        # give the joining peer info about the current DHT leader
        leader_info = self.peers[self.dht_leader]
        leader_tuple = {
            'peer_name': self.dht_leader,
            'ipv4_address': leader_info['ip'],
            'p_port': leader_info['p_port']
        }

        # also send the current dht peer list as tuples
        dht_tuples = []
        for pn in self.dht_peers:
            pi = self.peers[pn]
            dht_tuples.append({'peer_name': pn, 'ipv4_address': pi['ip'], 'p_port': pi['p_port']})

        print(f"   {name} joining dht, sending leader info ({self.dht_leader})")
        self.reply(addr, 'join-dht', 'SUCCESS', {
            'leader': leader_tuple,
            'dht_peers': dht_tuples,
            'year': self.dht_year
        })

    # --- DHT-REBUILT ---
    def handle_rebuilt(self, msg, addr):
        name = msg['peer_name']
        new_leader = msg.get('new_leader', '')
        new_peers = msg.get('new_dht_peers', [])  # list of peer names in new ring order

        if name != self.churn_peer:
            self.reply(addr, 'dht-rebuilt', 'FAILURE', {'reason': 'unexpected peer'})
            return

        # reset all old dht peers to Free first
        for p in self.dht_peers:
            if p in self.peers:
                self.peers[p]['state'] = 'Free'
                self.peers[p]['id'] = None

        # set the leaving peer to Free too
        if name in self.peers:
            self.peers[name]['state'] = 'Free'
            self.peers[name]['id'] = None

        # now set up the new ring
        self.dht_peers = new_peers
        self.dht_leader = new_leader
        self.dht_size = len(new_peers)

        for idx, pn in enumerate(new_peers):
            if pn in self.peers:
                if idx == 0:
                    self.peers[pn]['state'] = 'Leader'
                else:
                    self.peers[pn]['state'] = 'InDHT'
                self.peers[pn]['id'] = idx

        self.waiting_for = None
        self.churn_peer = None
        self.churn_action = None
        print(f"   dht rebuilt: leader={new_leader}, size={self.dht_size}")
        print(f"   new ring: {new_peers}")
        self.show_peers()
        self.reply(addr, 'dht-rebuilt', 'SUCCESS')

    # --- DEREGISTER ---
    def handle_deregister(self, msg, addr):
        name = msg['peer_name']
        if name not in self.peers:
            self.reply(addr, 'deregister', 'FAILURE', {'reason': 'who?'})
            return
        if self.peers[name]['state'] in ('InDHT', 'Leader'):
            self.reply(addr, 'deregister', 'FAILURE',
                       {'reason': 'cant deregister while in DHT'})
            return
        del self.peers[name]
        print(f"   removed {name}")
        self.reply(addr, 'deregister', 'SUCCESS')

    # --- TEARDOWN-DHT ---
    def handle_teardown(self, msg, addr):
        name = msg['peer_name']
        if name != self.dht_leader:
            self.reply(addr, 'teardown-dht', 'FAILURE', {'reason': 'only leader can teardown'})
            return
        self.waiting_for = 'teardown-complete'
        print(f"   teardown initiated by {name}")
        self.reply(addr, 'teardown-dht', 'SUCCESS')

    def handle_teardown_done(self, msg, addr):
        name = msg['peer_name']
        if name != self.dht_leader:
            self.reply(addr, 'teardown-complete', 'FAILURE', {'reason': 'not leader'})
            return
        for p in self.dht_peers:
            if p in self.peers:
                self.peers[p]['state'] = 'Free'
                self.peers[p]['id'] = None
        self.dht_exists = False
        self.dht_leader = None
        self.dht_size = 0
        self.dht_year = None
        self.dht_peers = []
        self.waiting_for = None
        print("   teardown done, everything reset")
        self.show_peers()
        self.reply(addr, 'teardown-complete', 'SUCCESS')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <port>")
        sys.exit(1)

    mgr = Manager(int(sys.argv[1]))
    try:
        mgr.run()
    except KeyboardInterrupt:
        print("\nbye")
