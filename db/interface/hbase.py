#      hbase.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
#      
#      This program is free software; you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation; either version 2 of the License, or
#      (at your option) any later version.
#      
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#      
#      You should have received a copy of the GNU General Public License
#      along with this program; if not, write to the Free Software
#      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#      MA 02110-1301, USA.

from database import Database
from happybase import ConnectionPool
from multiprocessing import cpu_count

POOL_SIZE = cpu_count() * 2

class HBase(Database):
    def connect(self, addr):
        return ConnectionPool(size=POOL_SIZE, host=addr)
    
    def get_table(self, conn, table):
        return conn.table(table)
    
    def open(self, collection):
        return collection.connection()
    
    def find(self, table, key):
        return table.row(key)
    
    def update(self, table, key, val):
        table.put(key, val)
