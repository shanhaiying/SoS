#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import glob
import os
import sys
import shutil
import subprocess
import unittest

from sos._version import __version__
from sos.parser import SoS_Script
from sos.targets import file_target
from sos.utils import env
# if the test is imported under sos/test, test interacive executor
from sos.workflow_executor import Base_Executor


def multi_attempts(fn):
    def wrapper(*args, **kwargs):
        for n in range(4):
            try:
                fn(*args, **kwargs)
                break
            except Exception:
                if n > 1:
                    raise
    return wrapper


class TestExecute(unittest.TestCase):
    def setUp(self):
        env.reset()
        subprocess.call('sos remove -s', shell=True)
        # self.resetDir('~/.sos')
        self.temp_files = []

    def tearDown(self):
        for f in self.temp_files:
            if file_target(f).exists():
                file_target(f).unlink()

    def touch(self, files):
        '''create temporary files'''
        if isinstance(files, str):
            files = [files]
        #
        for f in files:
            with open(f, 'w') as tmp:
                tmp.write('test')
        #
        self.temp_files.extend(files)

    def resetDir(self, dirname):
        if os.path.isdir(os.path.expanduser(dirname)):
            shutil.rmtree(os.path.expanduser(dirname))
        os.mkdir(os.path.expanduser(dirname))


    def testGroupWith(self):
        '''Test option group_with '''
        self.touch(['a.txt', 'b.txt'])
        for ofile in ['a.txt1', 'b.txt2']:
            if file_target(ofile).exists():
                file_target(ofile).unlink()
        #
        # string input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
vars = [1, 2]

input: files, group_with='vars', group_by=1
output: f"{_input}{_vars}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1', 'b.txt2']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()
        #
        # list input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
vars = [1]
vars2 = ['a']

input: files, group_with=('vars', 'vars2'), group_by=2
output: f"{_input[0]}{_vars}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()
        #
        # dict input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
input: files, group_with={'var': [1], 'var2': ['a']}, group_by=2
output: f"{_input[0]}{var}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()


    def testGroupWithAsTargetProperty(self):
        '''Test option group_with '''
        self.touch(['a.txt', 'b.txt'])
        for ofile in ['a.txt1', 'b.txt2']:
            if file_target(ofile).exists():
                file_target(ofile).unlink()
        #
        # string input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
vars = [1, 2]

input: files, group_with='vars', group_by=1
output: f"{_input}{_input._vars}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1', 'b.txt2']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()
        #
        # list input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
vars = [1]
vars2 = ['a']

input: files, group_with=('vars', 'vars2'), group_by=2
output: f"{_input[0]}{_input._vars}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()
        #
        # dict input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
input: files, group_with={'var': [1], 'var2': ['a']}, group_by=2
output: f"{_input[0]}{_input.var}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()


    def testInputPattern(self):
        '''Test option pattern of step input '''
        #env.verbosity = 4
        self.touch(['a-20.txt', 'b-10.txt'])
        script = SoS_Script(r"""
[0: shared=['base', 'name', 'par', '_output']]

files = ['a-20.txt', 'b-10.txt']
input: files, pattern=['{name}-{par}.txt', '{base}.txt']
output: ['{}-{}-{}.txt'.format(x,y,z) for x,y,z in zip(_base, _name, _par)]

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['base'], ["a-20", 'b-10'])
        self.assertEqual(env.sos_dict['name'], ["a", 'b'])
        self.assertEqual(env.sos_dict['par'], ["20", '10'])
        self.assertEqual(env.sos_dict['_output'], [
                         "a-20-a-20.txt", 'b-10-b-10.txt'])

    def testInputPatternAsTargetProperty(self):
        '''Test option pattern of step input '''
        #env.verbosity = 4
        self.touch(['a-20.txt', 'b-10.txt'])
        script = SoS_Script(r"""
[0: shared=['_output']]

files = ['a-20.txt', 'b-10.txt']
input: files, pattern=['{name}-{par}.txt', '{base}.txt']
output: [f'{x._base}-{x._name}-{x._par}.txt' for x in _input]

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['_output'], [
                         "a-20-a-20.txt", 'b-10-b-10.txt'])




if __name__ == '__main__':
    unittest.main()
