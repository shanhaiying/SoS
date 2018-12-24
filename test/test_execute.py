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

    def testCommandLine(self):
        '''Test command line arguments'''
        with open('test_cl.sos', 'w') as cl:
            cl.write('''\
#!/usr/bin/env sos-runner
#fileformat=SOS1.0

[L]
a =1
''')
        result = subprocess.check_output(
            'sos --version', stderr=subprocess.STDOUT, shell=True).decode()
        self.assertTrue(result.startswith('sos {}'.format(__version__)))
        self.assertEqual(subprocess.call('sos', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos -h', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos run -h', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        #
        self.assertEqual(subprocess.call('sos run test_cl -w -W',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 1)
        self.assertEqual(subprocess.call('sos-runner -h', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos dryrun -h', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos dryrun test_cl',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos dryrun test_cl.sos',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos dryrun test_cl L',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos-runner test_cl L',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        # script help
        self.assertEqual(subprocess.call('sos-runner test_cl -h',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos convert -h', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        #self.assertEqual(subprocess.call('sos convert sos-ipynb -h', stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        #
        self.assertEqual(subprocess.call('sos config -h', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos config -g --get',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos config', stderr=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos config --get',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos config -g --set a 5',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos config --get a',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        self.assertEqual(subprocess.call('sos config -g --unset a',
                                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True), 0)
        #

    def testInterpolation(self):
        '''Test string interpolation during execution'''
        self.touch(['a_1.txt', 'b_2.txt', 'c_2.txt'])
        script = SoS_Script(r"""
[0: shared='res']
res = ''
b = 200
res += f"{b}"
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['res'], '200')
        #
        script = SoS_Script(r"""
[0: shared='res']
res = ''
for b in range(5):
    res += f"{b}"
""")
        wf = script.workflow()
        Base_Executor(wf).run()
        self.assertEqual(env.sos_dict['res'], '01234')
        #
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
input: 'a_1.txt', 'b_2.txt', 'c_2.txt', pattern='{name}_{model}.txt'
output: [f'{x}_{y}_processed.txt' for x,y in zip(name, model)]

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'],  ['a_1_processed.txt',
                                                'b_2_processed.txt', 'c_2_processed.txt'])
        #
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
input: 'a_1.txt', 'b_2.txt', 'c_2.txt', pattern='{name}_{model}.txt'
output: [f"{x}_{y}_process.txt" for x,y in zip(name, model)]

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'],  ['a_1_process.txt',
                                                'b_2_process.txt', 'c_2_process.txt'])
        #
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
def add_a(x):
    return ['a'+_x for _x in x]

input: 'a_1.txt', 'b_2.txt', 'c_2.txt', pattern='{name}_{model}.txt'
output: add_a([f"{x}_{y}_process.txt" for x,y in zip(name, model)])

""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['res'],  ['aa_1_process.txt',
                                                'ab_2_process.txt', 'ac_2_process.txt'])

    def testGlobalVars(self):
        '''Test SoS defined variables'''
        script = SoS_Script(r"""
[0]
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['SOS_VERSION'], __version__)

    def testFuncDef(self):
        '''Test defintion of function that can be used by other steps'''
        self.touch(['aa.txt', 'ab.txt'])
        script = SoS_Script(r"""
def myfunc(a):
    sum(range(5))
    return ['a' + x for x in a]

[0: shared={'test':'step_input'}]
input: myfunc(['a.txt', 'b.txt'])
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertEqual(env.sos_dict['test'], ['aa.txt', 'ab.txt'])

    def testInput(self):
        '''Test input specification'''
        self.touch(['test_input.txt', 'test_input1.txt'])
        script = SoS_Script(r"""
[0: shared={'res':'step_output'}]
input: '*.txt'
output: [x + '.res' for x in _input]
""")
        wf = script.workflow()
        Base_Executor(wf).run(mode='dryrun')
        self.assertTrue(file_target('test_input.txt.res').resolve()
                        in env.sos_dict['res'])
        self.assertTrue(file_target('test_input1.txt.res').resolve()
                        in env.sos_dict['res'])

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


if __name__ == '__main__':
    unittest.main()
