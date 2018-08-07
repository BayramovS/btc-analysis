#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  6 23:26:40 2018

@author: SuleymanBayramov
"""

#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 22:33:58 2018

@author: SuleymanBayramov
"""


from datetime import datetime 
import pandas as pd
import datetime
import pyodbc
import numpy as np
import igraph
from collections import defaultdict



class TupleWithNamedAttributes(object):

    def __getitem__(self, item):
        return self.unapply()[item]

    def unapply(self):
        pass

    def __str__(self):
        return self.__class__.__name__ + self.unapply().__str__()

    __repr__ = __str__


class Input(TupleWithNamedAttributes):

    # def __init__(self, pubkey_hash, tx_hash, pubkey_id):
    def __init__(self, pubkey_id):
        # self.pubkey_hash = pubkey_hash
        # self.tx_hash = tx_hash
        try:
            self.pubkey_id = int(pubkey_id)
        except:
            pass

    def unapply(self):
        # return self.pubkey_hash, self.tx_hash, self.pubkey_id
        return self.pubkey_id


class Output(TupleWithNamedAttributes):

    # def __init__(self, txout_value, txout_pos, pubkey_hash, pubkey_id):
    def __init__(self, txout_value, txout_pos, pubkey_id):
        self.txout_value = int(txout_value)
        self.txout_pos = int(txout_pos)
        # self.pubkey_hash = pubkey_hash
        if pubkey_id is None:
            self.pubkey_id = -1
        else:
            self.pubkey_id = int(pubkey_id)


    def unapply(self):
        # return self.txout_value, self.txout_pos, self.pubkey_hash, self.pubkey_id
        return self.txout_value, self.txout_pos, self.pubkey_id


class Transaction(TupleWithNamedAttributes):

    def __init__(self, sender, recipient, amount, timestamp, block_id):
        self.sender = sender
        self.recipient = recipient
        self.amount = amount
        self.timestamp = timestamp
        self.block_id = block_id

    def unapply(self):
        return self.sender, self.recipient, self.amount, self.timestamp, self.block_id


class User(object):
    pass


class LazyDict(object):

    def __init__(self, fn):
        self.mapping = dict()
        self.fn = fn

    def __getitem__(self, item):
        if not item in self.mapping:
            self.mapping[item] = self.fn(item)
        return self.mapping[item]


class BlockChainGraph(object):

    def __init__(self, connection):
        self.connection = connection
        self.merged_users = dict()
        self.address_to_user = dict()
        self.user_to_addresses = defaultdict(set)
        self.account_in_degree = LazyDict(self.number_of_inputs)
        self.transactions = set()
        self.users = set()

    def cursor(self):
        return self.connection.cursor()

    def tx_in(self, tx_id):
        cursor = self.cursor()
        cursor.execute(
            "select txout.pubkey_id from txin inner join txout inner join tx "
            "where txin.tx_id = ? and txin.tx_id = tx.tx_id and txin.txout_id = txout.txout_id", tx_id)
        return [Input(*r) for r in cursor]

    def tx_out(self, tx_id):
        cursor = self.cursor()
        cursor.execute(
            "select txout_value, txout_pos, txout.pubkey_id from txout where tx_id = ?", tx_id)
        return [Output(*r) for r in cursor]

    @property
    def max_pubkey_id(self):
        cursor = self.cursor()
        cursor.execute(
            "select max(pubkey_id) from pubkey")
        return int(cursor.fetchone()[0])

    def number_of_inputs(self, pubkey_id):
        cursor = self.cursor()
        cursor.execute(
            "select count(*) from txin inner join txout on txin.txout_id = txout.txout_id "
                            "where pubkey_id = ?", pubkey_id)
        return cursor.fetchone()[0]

    def associate_address_with_user(self, pubkey_id, user):
        self.address_to_user[pubkey_id] = user
        self.user_to_addresses[user].add(pubkey_id)
        return user

    def merge_users(self, pubkey_ids):
        merged_user = User()
        original_users = {self.address_to_user[pubkey_id] for pubkey_id in pubkey_ids}
        for user in original_users:
            for pubkey_id in self.user_to_addresses[user]:
                self.associate_address_with_user(pubkey_id, merged_user)
            self.merged_users[user] = merged_user
        return merged_user

    def associate_with_same_address(self, pubkey_ids):
        identified_accounts = {pubkey_id for pubkey_id in pubkey_ids if pubkey_id in self.address_to_user}
        unidentified_accounts = pubkey_ids - identified_accounts
        if len(identified_accounts) > 1:
            user = self.merge_users(identified_accounts)
        elif len(identified_accounts) == 1:
            user = self.address_to_user[[u for u in identified_accounts][0]]
        else:
            user = User()
        if len(unidentified_accounts) > 0:
            for pubkey_id in unidentified_accounts:
                self.associate_address_with_user(pubkey_id, user)
        return user

    def is_potential_change_address(self, pubkey_id):
        return self.account_in_degree[pubkey_id] == 1

    def record_transaction(self, transaction):
        assert type(transaction.sender) is User
        assert type(transaction.recipient) is User
        print(transaction)
        self.transactions.add(transaction)
        self.users.add(transaction.sender)
        self.users.add(transaction.recipient)

    def canonical_user(self, user):
        while user in self.merged_users:
            user = self.merged_users[user]
        return user

    @property
    def canonical_transactions(self):
        return {Transaction(self.canonical_user(tr.sender), self.canonical_user(tr.recipient),
                            tr.amount, tr.timestamp, tr.block_id)
                for tr in self.transactions}

    @property
    def canonical_users(self):
        return {self.canonical_user(u) for u in self.users}

    def map_accounts_onto_users(self, pubkey_ids):
        return [self.address_to_user[pubkey_id] if exists else self.associate_address_with_user(pubkey_id, User())
            for exists, pubkey_id in zip([pubkey_id in self.address_to_user for pubkey_id in pubkey_ids], pubkey_ids)]

    def parse_transaction(self, tx_id, timestamp=None, block_id=None):
        inputs = self.tx_in(tx_id)
        outputs = self.tx_out(tx_id)
        amounts = [output.txout_value for output in outputs]
        recipient_addresses = [output.pubkey_id for output in outputs]
        sending_addresses = {input.pubkey_id for input in inputs}
        sender = self.associate_with_same_address(sending_addresses)
        if len(outputs) > 1:
            potential_change_addresses = \
                [pubkey_id for pubkey_id in recipient_addresses if self.is_potential_change_address(pubkey_id)]
            if len(potential_change_addresses) == 1:
                change_address = potential_change_addresses[0]
                self.associate_address_with_user(change_address, sender)
                self.merge_users(sending_addresses | {change_address})
        recipients = self.map_accounts_onto_users(recipient_addresses)
        for recipient, amount in zip(recipients, amounts):
            self.record_transaction(Transaction(sender, recipient, amount, timestamp, block_id))


######################
            #added or modified by Suleyman
            
    def parse_transactions(self, tr_id_min, tr_id_max):
        for i in range(tr_id_min, tr_id_max):
            self.parse_transaction(i)
            
            
    def parse_blocks(self, block_min, block_max):
        #block_min = blocks[0]
        #block_max = blocks[1][0]
        for i in range(block_min, block_max):
            self.parse_block(i)


    def parse_block(self, block_id):
        cursor = self.cursor()
        cursor.execute("select block_nTime from block where block_id = ?", block_id)
        timestamp = int(cursor.fetchone()[0])
        cursor = self.cursor()
        cursor.execute("select distinct tx_id from txin_detail where block_id = ?", block_id)
        for row in cursor:
            self.parse_transaction(int(row[0]), timestamp=timestamp, block_id=block_id)
            
            
    def parse_block_ontime(self, start_time, end_time):
         cursor = self.cursor()
         cursor.execute("select block_id from block where from_unixtime(block_nTime) > ? and from_unixtime(block_nTime) <= ?",(start_time, end_time))
         blocks = cursor.fetchall()
         for block in blocks:
             self.parse_block(block)
        
            
    
    
    
    def parse_trs_in_block(self, block_id):
        cursor = self.cursor()
        cursor.execute("select distinct tx_id from txin_detail where block_id = ?", block_id)
        for row in cursor:
            self.parse_transaction(int(row[0]), timestamp=timestamp, block_id=block_id)
        
        
            
                        
            

    @property
    def as_graph_dict(self):
         return {
            'nodes': [id(v) for v in self.canonical_users],
            'edges': [{'source': id(edge.sender), 'target': id(edge.recipient), 'weight': edge.amount, 'timestamp:': edge.timestamp, 'block_id:': edge.block_id}
                      for edge in self.canonical_transactions]
        }

    @property
    def as_D3_graph_dict(self):
        return {
            'nodes': [{'id': str(id(v))} for v in self.canonical_users],
            'links': [{'source': str(id(edge.sender)), 'target': str(id(edge.recipient)), 'value': edge.amount}
                      for edge in self.canonical_transactions]
        }

    @property
    def as_igraph(self):
        vertices = self.canonical_users
        edges = self.canonical_transactions

        vertex_map = dict()
        for (i, vertex) in enumerate(vertices):
            vertex_map[vertex] = i

        graph = igraph.Graph(directed=True)
        graph.add_vertices(len(vertices))
        for edge in edges:
            graph.add_edge(vertex_map[edge.sender], vertex_map[edge.recipient], weight=edge.amount)
        return graph

    def export_to_neo4j(self, neo4j_graph):

        tx = neo4j_graph.begin()

        node_map = dict()
        for vertex in self.canonical_users:
            node = py2neo.Node("User", id=id(vertex))
            node_map[vertex] = node
            tx.create(node)

        for transaction in self.canonical_transactions:
            tx.create(py2neo.Relationship(node_map[transaction.sender], 'TRANSFER', node_map[transaction.recipient],
                                          amount=transaction.amount, timestamp=transaction.timestamp, block_id=transaction.block_id))

        tx.commit()
        
        
        
        
    


def create_network(tr_id_min, tr_id_max, filename='JSON2.graphml'):
    
    connection = pymysql.connect(host='localhost', user='root', passwd='Mama1995', db='ABE')

    bcg = BlockChainGraph(connection)
    bcg.parse_transactions(tr_id_min, tr_id_max)
    bcg.as_igraph.write_graphml(filename)
    
    return bcg



def create_graph(block_min, block_max, filename='J.graphml'):
    connection = pymysql.connect(host='localhost', user='root', passwd='Mama1995', db='ABE')

    bcg = BlockChainGraph(connection)
    bcg.parse_blocks(block_min, block_max)
    bcg.as_igraph.write_graphml(filename)
    
    return bcg




def create_graph_in_interval(start_date, end_date, filename='J.graphml'):
    connection = pymysql.connect(host='localhost', user='root', passwd='Mama1995', db='ABE')

    bcg = BlockChainGraph(connection)
    bcg.parse_block_ontime(start_date, end_date)
    bcg.as_igraph.write_graphml(filename)
    
    return bcg
  

def transitivity_global(start_date, periods, filename):
    connection = pymysql.connect(host='localhost', user='root', passwd='Mama1995', db='ABE')
    bcg = BlockChainGraph(connection)
    transitivity = []
    dates = generate_periods(start_date, periods)
    for i in dates:
        bcg.parse_block_ontime(*i)
        transitivity.append(bcg.as_igraph.transitivity_undirected())
    bcg.as_igraph.write_graphml(filename)
    return transitivity
    
        
def generate_periods(start_date, periods):
    strt = pd.to_datetime(start_date)
    start = pd.date_range(strt, periods=periods, freq='15D')
    dates = []
    #dates.append((strt.date(), start[0].date()))
    for i in range(len(start[1:])):
        dates.append((start_date, start[i].date()))
    dates.pop(0)
    
    return dates

dates = generate_periods('2010-01-01', 24)

connection = pyodbc.connect("DRIVER={/usr/local/lib/libmyodbc8w.so}; SERVER=localhost; PORT=3306;DATABASE=abe; UID=abe; PASSWORD=th0rnxtc")


coefficient = []

for i in dates:
    bcg = BlockChainGraph(connection)
    bcg.parse_block_ontime(*i)
    coefficient.append(bcg.as_igraph.transitivity_undirected())
    

print(coefficient)


