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

    def testForEach(self):
        '''Test for_each option of input'''
        self.touch(['a.txt', 'b.txt', 'a.pdf'])
        script = SoS_Script(r"""
[0: shared=['counter', 'all_names', 'all_loop']]
files = ['a.txt', 'b.txt']
names = ['a', 'b', 'c']
c = ['1', '2']
counter = 0
all_names = ''
all_loop = ''

input: 'a.pdf', files, group_by='single', paired_with='names', for_each='c'

all_names += str(_names[0]) + " "
all_loop += str(_c) + " "

counter = counter + 1
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['counter'], 6)
        self.assertEqual(env.sos_dict['all_names'], "a b c a b c ")
        self.assertEqual(env.sos_dict['all_loop'], "1 1 1 2 2 2 ")
        #
        # test same-level for loop and parameter with nested list
        script = SoS_Script(r"""
[0: shared=['processed']]
files = ['a.txt', 'b.txt']
par = [(1, 2), (1, 3), (2, 3)]
res = ['p1.txt', 'p2.txt', 'p3.txt']
processed = []

input: files, for_each='par,res'
output: res, group_by=1

processed.append((_par, _res))
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['processed'], [
                         ((1, 2), 'p1.txt'), ((1, 3), 'p2.txt'), ((2, 3), 'p3.txt')])
        #
        # test for each for pandas dataframe
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])

input: for_each='data'
output: f"{_data['A']}_{_data['B']}_{_data['C']}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], [
                         '1_2_Hello.txt', '2_4_World.txt'])

        # test dictionary format of for_each
        self.touch(['a.txt', 'b.txt', 'a.pdf'])
        script = SoS_Script(r"""
[0: shared=['counter', 'all_names', 'all_loop']]
files = ['a.txt', 'b.txt']
names = ['a', 'b', 'c']
counter = 0
all_names = ''
all_loop = ''

input: 'a.pdf', files, group_by='single', paired_with='names', for_each={'c':  ['1', '2']}

all_names += str(_names[0]) + " "
all_loop += c + " "

counter = counter + 1
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['counter'], 6)
        self.assertEqual(env.sos_dict['all_names'], "a b c a b c ")
        self.assertEqual(env.sos_dict['all_loop'], "1 1 1 2 2 2 ")
        #
        # test multi-key dictionary format of for_each
        self.touch(['a.txt'])
        script = SoS_Script(r"""
import itertools
[0: shared=['counter', 'all_names', 'all_loop']]
parameter: n = [300, 100]
parameter: p = [50, 200, 100]
parameter: outfile = ['1', '2', '3', '4', '5', '6']
counter = 0
all_names = ''
all_loop = ''
input: 'a.txt', group_by='single', for_each={'_n,_p': [(_n,_p) for _n,_p in itertools.product(n,p) if _n > _p]}

all_names += outfile[_index] + " "
all_loop += '{} {} '.format(_n, _p)
counter = counter + 1
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['counter'], 4)
        self.assertEqual(env.sos_dict['all_names'], "1 2 3 4 ")
        self.assertEqual(env.sos_dict['all_loop'],
                         "300 50 300 200 300 100 100 50 ")
        #
        # test same-level for loop and parameter with nested list
        script = SoS_Script(r"""
[0: shared=['processed']]
files = ['a.txt', 'b.txt']
processed = []

input: files, for_each={'par':[(1, 2), (1, 3), (2, 3)], 'res': ['p1.txt', 'p2.txt', 'p3.txt']}
output: res

processed.append((par, res))
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['processed'], [
                         ((1, 2), 'p1.txt'), ((1, 3), 'p2.txt'), ((2, 3), 'p3.txt')])
        #
        # test for each for pandas dataframe
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
input: for_each={'data': pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])}
output: f"{data['A']}_{data['B']}_{data['C']}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], [
                         '1_2_Hello.txt', '2_4_World.txt'])
        #
        # support for pands Series and Index types
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])
input: for_each={'A': data['A']}
output: f"a_{A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['a_1.txt', 'a_2.txt'])
        #
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])
data.set_index('C', inplace=True)
input: for_each={'A': data.index}
output: f"{A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['Hello.txt', 'World.txt'])

        # test for each of Series
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(0, 1, 'Ha'), (1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])

data.set_index('A', inplace=True)
data = data.tail(2)
input: for_each={'A': data['B']}
output: f"{A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['2.txt', '4.txt'])

        # test iterable
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(0, 1, 'Ha'), (1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])

data.set_index('A', inplace=True)
data = data.tail(2)
input: for_each={'A,B': zip(data['B'],data['C'])}
output: f"{A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['2.txt', '4.txt'])
        
    def testForEachAsTargetProperty(self):
        '''Test for_each option of input'''
        self.touch(['a.txt', 'b.txt', 'a.pdf'])
        script = SoS_Script(r"""
[0: shared=['counter', 'all_names', 'all_loop']]
files = ['a.txt', 'b.txt']
names = ['a', 'b', 'c']
c = ['1', '2']
counter = 0
all_names = ''
all_loop = ''

input: 'a.pdf', files, group_by='single', paired_with='names', for_each='c'

all_names += str(_input._names) + " "
all_loop += str(_input._c) + " "

counter = counter + 1
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['counter'], 6)
        self.assertEqual(env.sos_dict['all_names'], "a b c a b c ")
        self.assertEqual(env.sos_dict['all_loop'], "1 1 1 2 2 2 ")
        #
        # test same-level for loop and parameter with nested list
        script = SoS_Script(r"""
[0: shared=['processed']]
files = ['a.txt', 'b.txt']
par = [(1, 2), (1, 3), (2, 3)]
res = ['p1.txt', 'p2.txt', 'p3.txt']
processed = []

input: files, for_each='par,res'
output: res, group_by=1

print([x._dict for x in step_input._groups])
processed.append((_input._par, _input._res))
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['processed'], [
                         ((1, 2), 'p1.txt'), ((1, 3), 'p2.txt'), ((2, 3), 'p3.txt')])
        #
        # test for each for pandas dataframe
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])

input: for_each='data'
print([x._dict for x in step_input._groups])
output: f"{_input._data['A']}_{_input._data['B']}_{_input._data['C']}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], [
                         '1_2_Hello.txt', '2_4_World.txt'])

        # test dictionary format of for_each
        self.touch(['a.txt', 'b.txt', 'a.pdf'])
        script = SoS_Script(r"""
[0: shared=['counter', 'all_names', 'all_loop']]
files = ['a.txt', 'b.txt']
names = ['a', 'b', 'c']
counter = 0
all_names = ''
all_loop = ''

input: 'a.pdf', files, group_by='single', paired_with='names', for_each={'c':  ['1', '2']}

all_names += str(_input._names) + " "
all_loop += _input.c + " "

counter = counter + 1
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['counter'], 6)
        self.assertEqual(env.sos_dict['all_names'], "a b c a b c ")
        self.assertEqual(env.sos_dict['all_loop'], "1 1 1 2 2 2 ")
        #
        # test multi-key dictionary format of for_each
        self.touch(['a.txt'])
        script = SoS_Script(r"""
import itertools
[0: shared=['counter', 'all_names', 'all_loop']]
parameter: n = [300, 100]
parameter: p = [50, 200, 100]
parameter: outfile = ['1', '2', '3', '4', '5', '6']
counter = 0
all_names = ''
all_loop = ''
input: 'a.txt', group_by='single', for_each={'_n,_p': [(_n,_p) for _n,_p in itertools.product(n,p) if _n > _p]}

all_names += outfile[_index] + " "
all_loop += '{} {} '.format(_input._n, _input._p)
counter = counter + 1
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['counter'], 4)
        self.assertEqual(env.sos_dict['all_names'], "1 2 3 4 ")
        self.assertEqual(env.sos_dict['all_loop'],
                         "300 50 300 200 300 100 100 50 ")
        #
        # test same-level for loop and parameter with nested list
        script = SoS_Script(r"""
[0: shared=['processed']]
files = ['a.txt', 'b.txt']
processed = []

input: files, for_each={'par':[(1, 2), (1, 3), (2, 3)], 'res': ['p1.txt', 'p2.txt', 'p3.txt']}
output: _input.res

processed.append((_input.par, _input.res))
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['processed'], [
                         ((1, 2), 'p1.txt'), ((1, 3), 'p2.txt'), ((2, 3), 'p3.txt')])
        #
        # test for each for pandas dataframe
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
input: for_each={'data': pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])}
output: f"{_input.data['A']}_{_input.data['B']}_{_input.data['C']}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], [
                         '1_2_Hello.txt', '2_4_World.txt'])
        #
        # support for pands Series and Index types
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])
input: for_each={'A': data['A']}
output: f"a_{_input.A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['a_1.txt', 'a_2.txt'])
        #
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])
data.set_index('C', inplace=True)
input: for_each={'A': data.index}
output: f"{_input.A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['Hello.txt', 'World.txt'])

        # test for each of Series
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(0, 1, 'Ha'), (1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])

data.set_index('A', inplace=True)
data = data.tail(2)
input: for_each={'A': data['B']}
output: f"{_input.A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['2.txt', '4.txt'])

        # test iterable
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
import pandas as pd
data = pd.DataFrame([(0, 1, 'Ha'), (1, 2, 'Hello'), (2, 4, 'World')], columns=['A', 'B', 'C'])

data.set_index('A', inplace=True)
data = data.tail(2)
input: for_each={'A,B': zip(data['B'],data['C'])}
output: f"{_input.A}.txt"
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'], ['2.txt', '4.txt'])

    def testGroupByWithNoInput(self):
        '''Test group_by with no input file'''
        script = SoS_Script(r'''
[0]
input: group_by=2
''')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testPairedWith(self):
        '''Test option paired_with '''
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

input: files, paired_with='vars', group_by=1
output: f"{_input}{_vars[0]}"
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
vars = [1, 2]
vars2 = ['a', 'b']

input: files, paired_with=('vars', 'vars2'), group_by=1
output: f"{_input}{_vars[0]}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1', 'b.txt2']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()
        #
        # dict input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
input: files, paired_with={'var': [1,2], 'var2': ['a', 'b']}, group_by=1
output: f"{_input}{var[0]}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1', 'b.txt2']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()


    def testPairedWithAsTargetProperty(self):
        '''Test option paired_with with values accessed by individual target '''
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

input: files, paired_with='vars', group_by=1
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
vars = [1, 2]
vars2 = ['a', 'b']

input: files, paired_with=('vars', 'vars2'), group_by=1
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
        # dict input
        script = SoS_Script(r'''
[0]
files = ['a.txt', 'b.txt']
input: files, paired_with={'var': [1,2], 'var2': ['a', 'b']}, group_by=1
output: f"{_input}{_input.var}"
run: expand=True
    touch {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for ofile in ['a.txt1', 'b.txt2']:
            self.assertTrue(file_target(ofile).target_exists('target'))
            file_target(ofile).unlink()


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

    def testOutputPattern(self):
        '''Test option pattern of step output'''
        #env.verbosity = 4
        self.touch(['a-20.txt', 'b-10.txt'])
        script = SoS_Script(r"""
[0: shared=['base', 'name', 'par', '_output']]

files = ['a-20.txt', 'b-10.txt']
input: files, pattern=['{name}-{par}.txt', '{base}.txt']
output: expand_pattern('{base}-{name}-{par}.txt'), expand_pattern('{par}.txt')

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['base'], ["a-20", 'b-10'])
        self.assertEqual(env.sos_dict['name'], ["a", 'b'])
        self.assertEqual(env.sos_dict['par'], ["20", '10'])
        self.assertEqual(env.sos_dict['_output'], ['a-20-a-20.txt',
                                                   'b-10-b-10.txt', '20.txt', '10.txt'])

    def testOutputFromInput(self):
        '''Test deriving output files from input files'''
        self.touch(['a.txt', 'b.txt'])
        script = SoS_Script(r"""
[0: shared={'counter':'counter', 'step':'step_output'}]
files = ['a.txt', 'b.txt']
counter = 0

input: files, group_by='single'
output: _input[0] + '.bak'
_output.touch()
counter += 1
""")
        wf = script.workflow()
        Base_Executor(wf, config={'sig_mode': 'force'}).run(mode='run')
        self.assertEqual(env.sos_dict['counter'], 2)
        self.assertEqual(env.sos_dict['step'], ['a.txt.bak', 'b.txt.bak'])

    def testLocalNamespace(self):
        '''Test if steps are well separated.'''
        # interctive mode behave differently
        self.touch('a.txt')
        script = SoS_Script(r"""
[1]
a = 1

[2]
# this should fail because a is defined in another step
print(a)

""")
        wf = script.workflow()
        self.assertRaises(Exception, Base_Executor(wf).run)
        # however, alias should be sent back
        script = SoS_Script(r"""
[1: shared={'shared': 'step_output'}]
input: 'a.txt'
output: 'b.txt'

[2: shared={'tt':'step_output'}]
print(shared)

output: [x + '.res' for x in shared]

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['shared'], ['b.txt'])
        self.assertEqual(env.sos_dict['tt'], ['b.txt.res'])
        #
        # this include other variables set in the step
        script = SoS_Script(r"""
[1: shared={'shared':'c', 'd':'d'}]
input: 'a.txt'
output: 'b.txt'

c = 'c.txt'
d = 1

[2: shared={'d': 'e'}]
# this should fail because a is defined in another step
print(shared)

output: shared

e = d + 1

""")
        wf = script.workflow()
        # I would like to disallow accessing variables defined
        # in other cases.
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['shared'], 'c.txt')
        self.assertEqual(env.sos_dict['d'], 2)

#
#    def testCollectionOfErrors(self):
#        '''Test collection of errors when running in dryrun mode.'''
#        script = SoS_Script('''
# [0]
#depends: executable('a1')
# [1: skip='blah']
#depends: executable('a2')
#
# [2]
#input: None
#depends: executable('a3')
# [3]
#depends: executable('a4')
#
# ''')
#        wf = script.workflow()
#        # we should see a single error with 2 messages.
#        # because 2 being on a separate branch will be executed but
#        # the later steps will not be executed
#        try:
#            Base_Executor(wf).run(mode='dryrun')
#        except Exception as e:
#            self.assertEqual(len(e.errors), 3)

    def testDynamicOutput(self):
        '''Testing dynamic output'''
        #
        if not os.path.isdir('temp'):
            os.mkdir('temp')
        #
        script = SoS_Script('''
[10: shared={'test':'step_output'}]
ofiles = []
output: dynamic(ofiles)

for i in range(4):
    ff = 'temp/something{}.html'.format(i)
    ofiles.append(ff)
    with open(ff, 'w') as h:
       h.write('a')
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['test'], [
                         'temp/something{}.html'.format(x) for x in range(4)])
        #
        shutil.rmtree('temp')

    def testDynamicInput(self):
        '''Testing dynamic input'''
        #
        if os.path.isdir('temp'):
            shutil.rmtree('temp')
        os.mkdir('temp')
        #
        script = SoS_Script('''
[1]

from pathlib import Path
for i in range(5):
    Path(os.path.join('temp', f'test_{i}.txt')).touch()

[10: shared={'test':'step_output'}]
input: dynamic(os.path.join('temp', '*.txt')), group_by='single'
output: dynamic(os.path.join('temp', '*.txt.bak'))

run: expand=True
touch {_input}.bak
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['test'], [os.path.join(
            'temp', 'test_{}.txt.bak'.format(x)) for x in range(5)],
            f"Expecting {[os.path.join('temp', 'test_{}.txt.bak'.format(x)) for x in range(5)]} observed {env.sos_dict['test']}")
        # this time we use th existing signature
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['test'], [os.path.join(
            'temp', 'test_{}.txt.bak'.format(x)) for x in range(5)],
            f"Expecting {[os.path.join('temp', 'test_{}.txt.bak'.format(x)) for x in range(5)]} observed {env.sos_dict['test']}")
        #
        shutil.rmtree('temp')

    def testAssignmentAfterInput(self):
        '''Testing assignment after input should be usable inside step process.'''
        #
        if os.path.isdir('temp'):
            shutil.rmtree('temp')
        os.mkdir('temp')
        #
        env.config['sig_mode'] = 'ignore'
        script = SoS_Script('''
[1]
rep = range(5)
input:  for_each='rep'
output: f"temp/{_rep}.txt"

# ff should change and be usable inside run
ff = f"{_rep}.txt"
run: expand=True
echo {ff}
touch temp/{ff}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        shutil.rmtree('temp')

    def testUseOfRunmode(self):
        '''Test the use of run_mode variable in SoS script'''
        #
        if os.path.isdir('temp'):
            shutil.rmtree('temp')
        os.mkdir('temp')
        env.config['sig_mode'] = 'ignore'
        script = SoS_Script('''
[1: shared={'res': '_output'}]
import random
for i in range(3):
    with open(f"temp/test_{random.randint(1, 100000)}.txt", 'w') as res:
        res.write(str(i))

''')
        wf = script.workflow()
        Base_Executor(wf).run()
        # we should have 9 files
        files = glob.glob(os.path.join('temp', '*.txt'))
        self.assertEqual(len(files), 3)
        shutil.rmtree('temp')

    def testActionBeforeInput(self):
        '''Testing the execution of actions before input directive
        (variables such as _index should be made available). '''
        script = SoS_Script('''
[0]
run('echo "A"')
input:
''')
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')

    def testDuplicateIOFiles(self):
        '''Test interpretation of duplicate input/output/depends'''
        self.resetDir('temp')
        # Test duplicate input
        os.system('touch temp/1.txt')
        script = SoS_Script('''
[1]
input: ['temp/1.txt' for x in range(5)]
run: expand=True
  touch temp/{len(_input)}.input
        ''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertTrue(os.path.isfile('temp/5.input'))
        # Test duplicate output
        script = SoS_Script('''
[1]
output: ['temp/2.txt' for x in range(5)]
run: expand=True
  touch temp/2.txt
  touch temp/{len(_output)}.output
        ''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertTrue(os.path.isfile('temp/5.output'))
        # Test duplicate depends
        script = SoS_Script('''
[1]
input: 'temp/1.txt'
depends: ['temp/2.txt' for x in range(5)]
output: 'temp/3.txt'
run: expand=True
  touch temp/3.txt
  touch temp/{len(_depends)}.depends
        ''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertTrue(os.path.isfile(os.path.join('temp', '5.depends')))
        shutil.rmtree('temp')

    def testOutputInLoop(self):
        '''Test behavior of {_output} when used in loop'''
        if os.path.isdir('temp'):
            shutil.rmtree('temp')
        os.mkdir('temp')
        env.config['sig_mode'] = 'ignore'
        script = SoS_Script('''
[default]
s = [x for x in range(5)]
output_files = ['temp/{}.txt'.format(x) for x in range(5)]
input: for_each = ['s'], concurrent=False
output: output_files[_index]
run: active = 0
rm -f temp/out.log
run: expand=True
echo {step_output} >> temp/out.log
touch {step_output}
        ''')
        wf = script.workflow()
        Base_Executor(wf).run()
        # output should have 1, 2, 3, 4, 5, respectively, and
        # the total record files would be 1+2+3+4+5=15
        with open('temp/out.log') as out:
            self.assertEqual(len(out.read().split()), 15)
        shutil.rmtree('temp')
        #
        os.mkdir('temp')
        script = SoS_Script('''
[default]
s = [x for x in range(5)]
output_files = ['temp/{}.txt'.format(x) for x in range(5)]
input: for_each = ['s']
output: output_files[_index]
run: active = 0
rm -f temp/out.log
run: expand=True
echo {step_output} >> temp/out.log
touch {step_output}
        ''')
        wf = script.workflow()
        env.config['sig_mode'] = 'ignore'
        Base_Executor(wf).run()
        with open('temp/out.log') as out:
            self.assertEqual(len(out.read().split()), 15)
        shutil.rmtree('temp')

    @multi_attempts
    def testExecutionLock(self):
        '''Test execution lock of two processes'''
        with open('lock.sos', 'w') as lock:
            lock.write(r'''
import time
[A_1]
output: 'a.txt'
with open('a.txt', 'w') as txt:
    txt.write('A1\n')

# A1 and A2 are independent
[A_2]
input: None
output: 'b.txt'
with open('b.txt', 'w') as txt:
    txt.write('A2\n')
        ''')
        ret1 = subprocess.Popen('sos run lock -j1', shell=True)
        ret2 = subprocess.Popen('sos run lock -j1', shell=True)
        ret1.wait()
        ret2.wait()
        # two processes execute A_1 and A_2 separately, usually
        # takes less than 5 seconds
        file_target('lock.sos').unlink()

    def testRemovedIntermediateFiles(self):
        '''Test behavior of workflow with removed internediate files'''
        for file in ('a.txt', 'aa.txt'):
            if file_target(file).exists():
                file_target(file).unlink()
        script = SoS_Script('''
[10]
output: 'a.txt'
run:
    echo "a" > a.txt

[20]
output: 'aa.txt'
run: expand=True
    cat {_input} > {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertTrue(file_target('aa.txt').target_exists())
        # rerun should be faster
        Base_Executor(wf).run()
        # if we remove the middle result, it should not matter
        os.remove('a.txt')
        Base_Executor(wf).run()
        #
        # if we remove the final result, it will be rebuilt
        os.remove('aa.txt')
        Base_Executor(wf).run()
        #
        # now we request the generation of target
        file_target('a.txt').unlink()
        file_target('aa.txt').unlink()
        Base_Executor(wf).run()
        #
        file_target('a.txt').unlink()
        file_target('aa.txt').unlink()

    def testStoppedOutput(self):
        '''test output with stopped step'''
        for file in ["{}.txt".format(a) for a in range(10)]:
            if file_target(file).exists():
                file_target(file).unlink()

        script = SoS_Script('''
[test_1]
input: for_each={'a': range(10)}
output: f"{a}.txt"

stop_if(a % 2 == 0)
run: expand=True
    touch {_output}

[test_2]
assert(len(step_input) == 5)
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for idx in range(10):
            if idx % 2 == 0:
                self.assertFalse(file_target(
                    "{}.txt".format(idx)).target_exists())
            else:
                self.assertTrue(file_target(
                    "{}.txt".format(idx)).target_exists())
                file_target(f"{idx}.txt").unlink()

    def testAllowError(self):
        '''Test option allow error'''
        if file_target('a.txt').exists():
            file_target('a.txt').unlink()
        script = SoS_Script('''
[test]
run:  allow_error=True
    something_wrong

run:
    touch a.txt
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertTrue(file_target('a.txt').target_exists())
        file_target('a.txt').unlink()

    def testConcurrentWorker(self):
        '''Test the starting of multiple workers #493 '''
        with open('test_script.sos', 'w') as script:
            script.write('''
[10]
input: for_each={'i': range(1)}

[20]
input: for_each={'i': range(2)}
''')
        subprocess.call('sos run test_script', shell=True)
        os.remove('test_script.sos')

    def testDependsCausedDependency(self):
        # test for #674
        for tfile in ('1.txt', '2.txt', '3.txt'):
            if file_target(tfile).exists():
                file_target(tfile).unlink()
        script = SoS_Script('''
[1: shared = {'dfile':'_output'}]
output: '1.txt'
run:
	echo 1 > 1.txt

[2: shared = {'ifile':'_output'}]
output: '2.txt'
run: expand=True
	echo {_input} > 2.txt

[3]
depends: ifile
input: dfile
output: '3.txt'
run: expand=True
	cat {_input} > {_output}
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for tfile in ('1.txt', '2.txt', '3.txt'):
            self.assertTrue(file_target(tfile).target_exists())
            if file_target(tfile).exists():
                file_target(tfile).unlink()

    def testConcurrentInputOption(self):
        '''Test input option'''
        self.touch(['1.txt', '2.txt'])
        script = SoS_Script('''
[1]
n =[str(x) for x in range(2)]
input: [f'{x+1}.txt' for x in range(2)], paired_with = 'n', concurrent = True
run: expand = True
  echo {_n} {_input}
''')
        wf = script.workflow()
        Base_Executor(wf).run()


    def testExecuteIPynb(self):
        '''Test extracting and executing workflow from .ipynb files'''
        script = SoS_Script(filename='sample_workflow.ipynb')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testOutputReport(self):
        '''Test generation of report'''
        if os.path.isfile('report.html'):
            os.remove('report.html')
        script = SoS_Script(r"""
[1: shared = {'dfile':'_output'}]
output: '1.txt'
run:
	echo 1 > 1.txt

[2: shared = {'ifile':'_output'}]
output: '2.txt'
run: expand=True
	echo {_input} > 2.txt

[3]
depends: ifile
input: dfile
output: '3.txt'
run: expand=True
	cat {_input} > {_output}
""")
        env.config['output_report'] = 'report.html'
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertTrue(os.path.isfile('report.html'))

    @unittest.skipIf(sys.platform == 'win32', 'Graphviz not available under windows')
    def testOutputReportWithDAG(self):
        # test dag
        if os.path.isfile('report.html'):
            os.remove('report.html')
        script = SoS_Script(r"""
[1: shared = {'dfile':'_output'}]
output: '1.txt'
run:
	echo 1 > 1.txt

[2: shared = {'ifile':'_output'}]
output: '2.txt'
run: expand=True
	echo {_input} > 2.txt

[3]
depends: ifile
input: dfile
output: '4.txt'
run: expand=True
	cat {_input} > {_output}
""")
        env.config['output_report'] = 'report.html'
        env.config['output_dag'] = 'report.dag'
        wf = script.workflow()
        Base_Executor(wf).run()
        with open('report.html') as rep:
            content = rep.read()
        self.assertTrue('Execution DAG' in content)

    def testSoSStepWithOutput(self):
        '''Test checking output of sos_step #981'''
        script = SoS_Script('''
[step]
output: 'a'
sh:
touch a

[default]
depends: sos_step('step')
''')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testMultiSoSStep(self):
        '''Test matching 'a_1', 'a_2' etc with sos_step('a')'''
        for file in ('a_1', 'a_2'):
            if file_target(file).exists():
                file_target(file).unlink()
        script = SoS_Script('''
[a_b_1]
output: "a_1"
sh:
  echo whatever > a_1

[a_b_2]
output: "a_2"
sh: expand=True
  cp {_input} {_output}

[default]
depends: sos_step('a_b')
''')
        wf = script.workflow()
        res = Base_Executor(wf).run()
        self.assertEqual(res['__completed__']['__step_completed__'], 3)
        self.assertTrue(os.path.isfile('a_1'))
        self.assertTrue(os.path.isfile('a_2'))
        with open('a_1') as a1, open('a_2') as a2:
            self.assertEqual(a1.read(), a2.read())

    def testDependsAuxiAndForward(self):
        '''Test depends on auxiliary, which then depends on a forward-workflow #983'''
        for f in ('a_1', 'a_2'):
            if file_target(f).exists():
                file_target(f).unlink()
        script = SoS_Script('''

[hg_1]
output: 'a_1'
sh:
  echo "something" > a_1

[hg_2]

[star: provides = "a_2"]
depends: sos_step('hg')
sh:
  cp  a_1 a_2

[default]
depends: "a_2"
        ''')
        wf = script.workflow()
        res = Base_Executor(wf).run()
        self.assertEqual(res['__completed__']['__step_completed__'], 4)
        self.assertTrue(os.path.isfile('a_1'))
        self.assertTrue(os.path.isfile('a_2'))
        with open('a_1') as a1, open('a_2') as a2:
            self.assertEqual(a1.read(), a2.read())

    def testDependsAuxiAndSingleStepForward(self):
        '''Test depends on auxiliary, which then depends on a single-step forward-workflow'''
        for f in ('a_1', 'a_2'):
            if file_target(f).exists():
                file_target(f).unlink()
        script = SoS_Script('''

[hg_1]
output: 'a_1'
sh:
  echo "something" > a_1

[star: provides = "a_2"]
depends: sos_step('hg')
sh:
  cp  a_1 a_2

[default]
depends: "a_2"
        ''')
        wf = script.workflow()
        res = Base_Executor(wf).run()
        self.assertEqual(res['__completed__']['__step_completed__'], 3)
        self.assertTrue(os.path.isfile('a_1'))
        self.assertTrue(os.path.isfile('a_2'))
        with open('a_1') as a1, open('a_2') as a2:
            self.assertEqual(a1.read(), a2.read())



    def testMultiDepends(self):
        '''Test a step with multiple depdendend steps'''
        for file in ('dbsnp.vcf', 'hg19.fa', 'f1.fastq', 'f2.fastq', 'f1.bam', 'f2.bam', 'f1.bam.idx', 'f2.bam.idx'):
           if os.path.isfile(file):
               os.remove(file)
        self.touch(['f1.fastq', 'f2.fastq'])
        script = SoS_Script('''
import time

[refseq: provides='hg19.fa']
time.sleep(1)
_output.touch()

[dbsnp: provides='dbsnp.vcf']
_output.touch()

[align_10]
depends: 'hg19.fa'
input: 'f1.fastq', 'f2.fastq', group_by=1, concurrent=True
output: _input.with_suffix('.bam')
_output.touch()

[align_20]
input: group_by=1, concurrent=True
output: _input.with_suffix('.bam.idx')
_output.touch()

[call_10]
depends: 'dbsnp.vcf', 'hg19.fa'

[call_20]
''')
        wf = script.workflow('align+call')
        Base_Executor(wf).run()
        for file in ('dbsnp.vcf', 'hg19.fa', 'f1.bam', 'f2.bam', 'f1.bam.idx', 'f2.bam.idx'):
            self.assertTrue(os.path.isfile(file))


    def testDependsToConcurrentSubstep(self):
        '''Testing forward style example'''
        # sos_variable('data') is passed to step [2]
        # but it is not passed to concurrent substep because
        # the variable is not used in the substep. This test
        # should fail at least under windows
        script = SoS_Script('''
[1: shared={'data': 'step_output'}]
output: 'a.txt'
_output.touch()

[2]
depends: sos_variable('data')
input: for_each={'i': range(2)}, group_by=1, concurrent=True
print(1)
''')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testPassOfTargetSource(self):
        '''Test passing of source information from step_output'''
        script = SoS_Script('''
[1]
output: 'a.txt'
_output.touch()

[2]
assert step_input.sources == ['1']
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        script = SoS_Script('''
[1]
input: for_each={'i': range(2)}
output: 'a.txt', 'b.txt', group_by=1
_output.touch()

[2]
assert step_input.sources == ['1', '1']
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        file_target('c.txt').touch()
        script = SoS_Script('''
[1]
input: for_each={'i': range(2)}
output: 'a.txt', 'b.txt', group_by=1
_output.touch()

[2]
input: 'c.txt'
assert step_input.sources == ['2']
''')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testRerunWithZap(self):
        script = SoS_Script('''
[step_10]
input: for_each={'i': range(3)}
output: f'zapped_example_{i}.txt'
sh: expand=True
  echo "hello" > {_output}

[step_20]
input: group_by=1
output: _input.with_suffix('.bak')
sh: expand=True
   cp {_input} {_output}

_input.zap()
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        script = SoS_Script('''
[step_10]
input: for_each={'i': range(3)}
output: f'zapped_example_{i}.txt'
sh: expand=True
  echo "hello" > {_output}

[step_20]
input: group_by=1
output: _input.with_suffix('.bak')
print(_output)
sh: expand=True
   cp {_input} {_output}

_input.zap()
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for i in range(3):
            os.remove(f'zapped_example_{i}.txt.zapped')

    def testReturn_OutputInStepOutput(self):
        '''Testing the return of _output as groups of step_output'''
        script = SoS_Script('''\
[1]
input: for_each=dict(i=range(5))
output: f'a_{i}.txt'
_output.touch()
assert(_input.i == i)

[2]
assert(len(step_input.groups) == 5)
assert(len(step_input) == 5)
assert(step_input.groups[0] == 'a_0.txt')
assert(step_input.groups[4] == 'a_4.txt')
#assert(_input.i == _index)
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        # test accumulation of named output
        script = SoS_Script('''\
[1]
input: for_each=dict(i=range(5))
output: a=f'a_{i}.txt', b=f'b_{i}.txt'
_output.touch()

[2]
assert(len(step_input.groups) == 5)
assert(len(step_input) == 10)
assert(step_input.groups[0] == ['a_0.txt', 'b_0.txt'])
assert(step_input.groups[0].sources == ['a', 'b'])
assert(step_input.groups[4] == ['a_4.txt', 'b_4.txt'])
assert(step_input.groups[4].sources == ['a', 'b'])
''')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testOutputFrom(self):
        '''Testing output_from input function'''
        script = SoS_Script('''\
[A]
input: for_each=dict(i=range(5))
output: f'a_{i}.txt'
_output.touch()

[A1]
input: for_each=dict(i=range(4))
output: aa=f'a_{i}.txt'
_output.touch()

[B]
input: output_from('A')
assert(len(step_input.groups) == 5)
assert(len(step_input) == 5)
assert(step_input.sources == ['A']*5)
assert(step_input.groups[0] == 'a_0.txt')
assert(step_input.groups[4] == 'a_4.txt')

[C]
input: K=output_from('A')
assert(len(step_input.groups) == 5)
assert(step_input.sources == ['K']*5)

[D]
input: K=output_from('A', group_by='all')
assert(len(step_input) == 5)
assert(len(step_input.groups) == 1)
assert(step_input.sources == ['K']*5)

[E]
input: output_from('A1', group_by='all')
assert(len(step_input) == 4)
assert(len(step_input.groups) == 1)
assert(step_input.sources == ['aa']*4)

[F]
input: K=output_from('A1', group_by='all')['aa']
assert(len(step_input) == 4)
assert(len(step_input.groups) == 1)
assert(step_input.sources == ['K']*4)

[G_0]
input: for_each=dict(i=range(4))
output: f'g_{i}.txt'
_output.touch()

[G_100]
input: K=output_from(-1, group_by=2)
assert(len(step_input) == 4)
assert(len(step_input.groups) == 2)
assert(step_input.sources == ['K']*4)

[H_0]
input: for_each=dict(i=range(4))
output: f'g_{i}.txt'
_output.touch()

[H_100]
input: K=output_from([-1, 'A1'], group_by=2)
assert(len(step_input) == 8)
assert(len(step_input.groups) == 4)
assert(step_input.sources == ['K']*8)

''')
        for wf in ('B', 'C', 'D', 'E', 'F', 'G', 'H'):
            wf = script.workflow(wf)
            Base_Executor(wf).run()


    def testNamedOutput(self):
        '''Testing named_output input function'''
        script = SoS_Script('''\

[A]
input: for_each=dict(i=range(4))
output: aa=f'a_{i}.txt', bb=f'b_{i}.txt'
_output.touch()

[B]
input: named_output('aa')
assert(len(step_input.groups) == 4)
assert(len(step_input) == 4)
assert(step_input.sources == ['aa']*4)
assert(step_input.groups[0] == 'a_0.txt')
assert(step_input.groups[3] == 'a_3.txt')

[C]
input: K=named_output('bb')
assert(len(step_input.groups) == 4)
assert(len(step_input) == 4)
assert(step_input.sources == ['K']*4)
assert(step_input.groups[0] == 'b_0.txt')
assert(step_input.groups[3] == 'b_3.txt')

[D]
input: K=named_output('bb', group_by=2)
assert(len(step_input.groups) == 2)
assert(len(step_input) == 4)
assert(step_input.sources == ['K']*4)
assert(step_input.groups[1] == ['b_2.txt', 'b_3.txt'])

''')
        for wf in ('B', 'C', 'D'):
            wf = script.workflow(wf)
            Base_Executor(wf).run()

if __name__ == '__main__':
    unittest.main()
