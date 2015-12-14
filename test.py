import unittest
import cStringIO as StringIO

import bqe

STMT_CREATE_OK=[
'''CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT * from [a1.b1];''',

'''CREATE TABLE [foo2.bar2]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT a, b, c from [a1.b1];''',
]

STMT_CREATE_BAD=[
'''CREATE TABLE [foo1.bar1]
USING bqe
AS 
SELECT * from [a1.b1];''',

'''CREATE TABLE [foo2.bar2]
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT a, b, c from [a1.b1];''',
]


class BqeBasicTest(unittest.TestCase):

  def test_1(self):
      stmt = '''CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT * from [a1.b1];'''
      st = bqe.StmtTranslatior(stmt)
      self.assertTrue(st.is_valid)
      cmd = st.bq_cmd()
      self.assertTrue(cmd[0], 'query')
      self.assertEqual(cmd[1], ['--udf_resource', 'gs://my-gs/udf/myfun1.js',  '--destination_table', 'foo1.bar1'])
      self.assertEqual(cmd[2], 'SELECT * from [a1.b1]')

  def test_2(self):
      stmt = '''CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( 
  udf_resource "gs://my-gs/udf/myfun1.js" 
  use_cache "false"
  append_table "true"
)
AS 
SELECT
* from
[a1.b1];'''
      st = bqe.StmtTranslatior(stmt)
      self.assertTrue(st.is_valid)
      cmd = st.bq_cmd()
      self.assertTrue(cmd[0], 'query')
      self.assertEqual(cmd[1], [
        '--udf_resource', 'gs://my-gs/udf/myfun1.js',
        '--nouse_cache',
        '--append_table',
        '--destination_table', 'foo1.bar1'])
      self.assertEqual(cmd[2], '''SELECT
* from
[a1.b1]''')


class BqeBasicCreateTest(unittest.TestCase):
  def test_good(self):
      for s in STMT_CREATE_OK:
        st = bqe.StmtTranslatior(s)
        self.assertTrue(st.is_valid())

  def test_bad(self):
      for s in STMT_CREATE_BAD:
        st = bqe.StmtTranslatior(s)
        self.assertFalse(st.is_valid())

class BqeJobRunnerBasicTest(unittest.TestCase):
  def test_1(self):
    stmt1 = '''CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT * from [a1.b1] where a = "b" and c = 'd';

CREATE TABLE [foo2.bar2]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT a, b, c
from [a1.b1];
'''
    jr = bqe.JobRunner([], [],
        stmt1, 
        self.__class__.__name__, True)
    jr.run()

   

  def test_acf(self):
    stmt = '''CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT * from [a1.b1] where a = "b" and c = 'd';
'''
    jr = bqe.JobRunner([], ['--nouse_cache', '-n', '0'],
        stmt, 
        self.__class__.__name__, True)
    jr.run()

if __name__ == '__main__':
    unittest.main()