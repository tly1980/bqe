#!/usr/bin/env python
import argparse
import re
import logging
import subprocess
import json
import sys

import shortuuid
import sqlparse


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-2s %(levelname)-2s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)


AP=argparse.ArgumentParser(description="bqe - bq extender. To convert your SQL into bq command.")
AP.add_argument('sql', nargs="?", default=None, type=str, help="SQL statement")
AP.add_argument('-f', nargs="?", default=None, type=str, help="SQL file")
AP.add_argument('--bqf', nargs="?", default=None, type=str, help="bq global flags. Please provide it as a single string. e.g. '--api_version 2 --dataset_id my_data' ")
AP.add_argument('--acf', nargs="?", default="--allow_large_results -n 10", type=str, help="action flags. Please provide it as a single string. e.g. '--max_rows 0 --dry_run' ")
AP.add_argument('--dry', action='store_true', help="Enable dry run.", default=False)
AP.add_argument('-t','--track', type=str, default=None, help="A track prefix. If you provide it, it will embeded into job_id as 't=foo'")
AP.add_argument('--skip', type=int, default=-1, help="How many statements to skip")

IGNORABLE = [
    sqlparse.tokens.Whitespace, 
    sqlparse.tokens.Newline,
    sqlparse.tokens.Comment,
]

IGNORABLE_CLS = [
    sqlparse.sql.Comment,
]

def expect(tk, ttype, value):
    if tk.ttype != ttype:
        return False

def strip_tokens(tks):
    ret = []
    for t in tks:
        if t.ttype:
            if t.ttype not in IGNORABLE:
                ret.append(t)
        else:
            if t.__class__ not in IGNORABLE_CLS:
                ret.append(t)

    return ret


RULE_STMT_CREATE_TABLE = strip_tokens(sqlparse.parse('''CREATE TABLE [*] 
USING bqe 
OPTIONS ( udf_resource "*" )
AS 
SELECT''')[0].tokens)


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
        self.re_options = re.compile('([a-zA-Z_-]+)\s+"([^"]*)"')
        #self.default_query_options=['--allow_large_results']

    def parse(self):
        self.stmt = sqlparse.parse(self.stmt_raw)[0]
        self.stmt_minimum_tokens = strip_tokens(self.stmt.tokens)

    def bq_action(self):
        if self.stmt_minimum_tokens[0].value.lower() in ('create', 'select'):
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
            raise InvalidSqlStmtException(self.stmt_raw)

        if self.bq_action() == 'query':
            return self._bq_cmd_query()

        elif self.bq_action() == 'load':
            return self._bq_cmd_load()

    def _bq_cmd_query(self):
        tk_begin = self.stmt.token_next_match(0, sqlparse.tokens.DDL,"create")
        tk_end = self.stmt.token_next_match(0, sqlparse.tokens.Keyword,"as")
        tks_striped = strip_tokens(self.stmt.tokens_between(tk_begin, tk_end))
        table = None
        options = []


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
            table=table.strip("[]")
            options += ['--destination_table', "%s" % table]

        tk_as = self.stmt.token_next_match(0, sqlparse.tokens.Keyword, "as")
        tk_as_idx = self.stmt.token_index(tk_as)
        stmt_str = ''.join([
            str(tk) for tk in self.stmt.tokens[tk_as_idx+1:]]
            ).strip().strip(';')

        return { 
            'action':'query',
            'cmds': ('query', options, stmt_str),
            'table': table,
            'stmt_str': stmt_str
        }

    def xtract_options(self, options_str):
        lst = self.re_options.findall(options_str)
        ret = []

        for kvp in lst:
            k, v = kvp
            if v not in ("true", "false"):
                ret += ['--%s' % k, '{v}'.format(v=v)]
            else:
                if v == "true":
                    ret += ['--%s' % k]
                else:
                    ret += ['--no%s' % k]

        return ret


class JobRunner(object):
    def __init__(self, 
        bq_global_flags, action_flags, stmts_raw, 
        job_id_pfx, is_dry, skip):
        self.bq_global_flags = bq_global_flags
        self.action_flags = action_flags
        self.stmts_raw = stmts_raw
        self.job_id_pfx = job_id_pfx

        self.is_dry = is_dry
        self.job_idx = 0
        self.outfile = sys.stdout
        self.jobs = []
        self.skip = skip


    def run(self):
        sqls = sqlparse.split(self.stmts_raw)
        for sql in sqls:
            sql = sql.strip()
            if sql:
                self.job_idx += 1
                st = StmtTranslatior(sql)
                self.execute(st.bq_cmd())

    def execute(self, bq_cmd):
        bq_cmd_tupple = bq_cmd['cmds']
        self.job_id_current = '%s_%s' % ( self.job_id_pfx, self.job_idx)
        actual_cmd = self.render_cmd(bq_cmd_tupple)
        #self.jobs[self.job_id_current] = actual_cmd
        self.jobs.append((str(self.job_id_current), actual_cmd))
        logging.info("about to execute: job_idx=%s, bq_job_id=%s, table=%s" % (self.job_idx, self.job_id_current, bq_cmd['table']))
        cmd_nosql = ' '.join(actual_cmd[:-1])
        if self.is_dry:
            self.outfile.write(cmd_nosql)
            self.outfile.write("\n======\n")
            self.outfile.write("%s\n======\n\n\n" % bq_cmd['stmt_str'])
            self.outfile.flush()
        else:
            if self.job_idx > self.skip:
                logging.info("Running: job_idx=%s, sql=\n%s" % (self.job_idx, bq_cmd['stmt_str']))
                r = self.bq_call(actual_cmd)
                if not r:
                    sys.exit(
                        "bq run failed: job_idx=%s, bq_job_id= %s" % (self.job_idx, self.job_id_current)
                        )
            else:
                logging.info("Skipping: %s, sql=%s" % (self.job_id_current, bq_cmd['stmt_str']))

    def bq_call(self, actual_cmd):
        ret = subprocess.call(actual_cmd)
        if ret == 0:
            return True
        return False


    def globl_flags(self):
        ret = list(self.bq_global_flags)
        ret += ['--job_id', self.job_id_current]
        return ret

    def render_cmd(self, bq_cmd_tupple):
        cmd = ['bq']
        cmd += self.globl_flags()
        cmd += [bq_cmd_tupple[0]]
        cmd += bq_cmd_tupple[1]
        cmd += self.action_flags

        cmd += ["{a}".format(a=bq_cmd_tupple[2])]

        return cmd

def get_job_idx_pfx(**kwargs):
    pfxparts = ['bqe', shortuuid.uuid()]
    for k, v in kwargs.items():
        pfxparts.append('%s-%s' % (k, v))
    return '_'.join(pfxparts)


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

    acf = []
    if args.acf:
        acf = args.acf.split()

    if args.track:
        job_id_pfx = get_job_idx_pfx(t=args.track)
    else:
        job_id_pfx = get_job_idx_pfx()

    jr = JobRunner(bqf, acf, sql, job_id_pfx, args.dry, args.skip)
    jr.run()

if __name__ == '__main__':
    main(AP.parse_args())
