#!/usr/bin/env python
import argparse
import re
import logging


import shortuuid
import sqlparse


AP=argparse.ArgumentParser(description="bqe - bq extender. To convert your SQL into bq command.")
AP.add_argument('sql', nargs="?", default=None, type=str, help="SQL statement")
AP.add_argument('-f', nargs="?", default=None, type=str, help="SQL file")
AP.add_argument('--bqf', nargs="?", default=None, type=str, help="bq global flags. Please provide it as a single string. e.g. '--api_version 2 --dataset_id my_data' ")
AP.add_argument('--dry', action='store_true', help="Enable dry run.", default=False)
AP.add_argument('--track', action='store_true', help="Enable tracking.", default=False)

IGNORABLE = [
    sqlparse.tokens.Whitespace, 
    sqlparse.tokens.Newline,
    sqlparse.tokens.Comment,
]

def expect(tk, ttype, value):
    if tk.ttype != ttype:
        return False

def strip_tokens(tks):
    return [t for t in tks if t.ttype not in IGNORABLE]


RULE_STMT_CREATE_TABLE = strip_tokens(sqlparse.parse('''CREATE TABLE [*] 
USING bqe 
OPTIONS ( udf_resource "*" )
AS 
SELECT''')[0].tokens)

RULE_STMT_LOAD_TABLE = strip_tokens(sqlparse.parse('''LOAD DATA INTO  ''')[0].tokens)


def rule_match(rule, tokens):
    for i, t in enumerate(rule):
        t2 = tokens[i]
        if t.ttype in [
            sqlparse.tokens.Keyword, sqlparse.tokens.DDL]:
            if not t.match(t2.ttype, t2.value, False):
                return False

        # do nothing for ttype = identifier

    return True

class InvalidSqlStmtException(Exception):
    pass

class UnspportBqAction(Exception):
    pass

class StmtTranslatior(object):
    def __init__(self, stmt_raw, strict=False):
        self.stmt_raw = stmt_raw.strip()
        self.strict=strict
        self.parse()
        self.re_options = re.compile('([a-zA-Z_-]+)\s+"([^"]+)"')
        self.default_query_options=['--allow_large_results']

    def parse(self):
        self.stmt = sqlparse.parse(self.stmt_raw)[0]
        self.stmt_minimum_tokens = strip_tokens(self.stmt.tokens)

    def bq_action(self):
        if self.stmt_raw.lower().startswith('create '):
            return 'query'

        raise UnspportBqAction()

        ## fix me haven't support load yet
        if self.stmt_raw.lower().startswith('load '):
            return 'load'

    def is_valid(self):
        # check create statement
        if rule_match(
            RULE_STMT_CREATE_TABLE, self.stmt_minimum_tokens):
            return True
        return False

    def bq_cmd(self):
        if not self.is_valid():
            raise InvalidSqlStmtException()

        if self.bq_action() == 'query':
            return self._bq_cmd_query()

        elif self.bq_action() == 'load':
            return self._bq_cmd_load()

    def _bq_cmd_query(self):
        tk_begin = self.stmt.token_next_match(0, sqlparse.tokens.DDL,"create")
        tk_end = self.stmt.token_next_match(0, sqlparse.tokens.Keyword,"as")
        tks_striped = strip_tokens(self.stmt.tokens_between(tk_begin, tk_end))
        table = None
        options = list(self.default_query_options)

        for idx, tk in enumerate(tks_striped):
            if not tk.match(sqlparse.tokens.Keyword,"as"):
                following_tk = tks_striped[idx+1]
                if tk.match(sqlparse.tokens.Keyword, 'table', False) and \
                    isinstance(following_tk, sqlparse.sql.Identifier):
                    table = following_tk.value
                if tk.match(sqlparse.tokens.Keyword, 'options', False) and \
                    isinstance(following_tk, sqlparse.sql.Parenthesis):
                    options = self.xtract_options(following_tk.value)
                    break

        if table:
            options += ['--destination_table', "%s" % repr(str(table))]

        tk_as = self.stmt.token_next_match(0, sqlparse.tokens.Keyword, "as")
        tk_as_idx = self.stmt.token_index(tk_as)
        stmt_str = ''.join([
            tk.value for tk in self.stmt.tokens[tk_as_idx+1:]]
            ).strip().strip(';')

        return ('query', options, stmt_str)

    def xtract_options(self, options_str):
        lst = self.re_options.findall(options_str)
        ret = []

        for kvp in lst:
            k, v = kvp
            ret += ['--%s' % k, repr(str(v))]

        return ret

class JobRunner(object):
    def __init__(self, bq_global_flags, stmts_raw, job_id_pfx, is_dry):
        self.bq_global_flags = bq_global_flags
        self.stmts_raw = stmts_raw
        self.job_id_pfx = job_id_pfx

        self.is_dry = is_dry
        self.job_idx = 0
        self.jobs = {}


    def run(self):
        for sql in sqlparse.split(self.stmts_raw):
            sql = sql.strip()
            if sql:
                self.job_idx += 1
                st = StmtTranslatior(sql)
                self.execute(st.bq_cmd())

    def execute(self, bq_cmd_tupple):
        self.job_id_current = '%s_%s' % ( self.job_id_pfx, self.job_idx)
        actual_cmd = self.render_cmd(bq_cmd_tupple)
        self.jobs[self.job_id_current] = actual_cmd
        if self.is_dry:
            print ' '.join(actual_cmd)
        else:
            sh_call(actual_cmd)

    def globl_flags(self):
        ret = list(self.bq_global_flags)
        if self.job_id_pfx:
            ret += ['--job_id', self.job_id_current]
        return ret

    def render_cmd(self, bq_cmd_tupple):
        cmd = ['bq']
        cmd += self.globl_flags()
        cmd += [bq_cmd_tupple[0]]
        cmd += bq_cmd_tupple[1]

        cmd += ["%s" % repr(bq_cmd_tupple[2])]

        return cmd


def main(args):
    sql = None
    if args.sql:
        sql = arg.sql
    elif args.f:
        with open(args.f) as f:
            sql = f.read()

    if not sql:
        sys.exit("Please provide sql statement")

    bqf = []
    if args.bqf:
        bqf = args.bqf.split()

    job_id_pfx = None
    if args.track:
        job_id_pfx = shortuuid.uuid()

    jr = JobRunner(bqf, sql, job_id_pfx, args.dry)
    jr.run()

if __name__ == '__main__':
    main(AP.parse_args())
