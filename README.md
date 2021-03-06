[![PyPI version](https://badge.fury.io/py/sos.svg)](https://badge.fury.io/py/sos)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1291524.svg)](https://doi.org/10.5281/zenodo.1291524)
[![Join the chat at https://gitter.im/vatlab/SoS](https://badges.gitter.im/vatlab/SoS.svg)](https://gitter.im/vatlab/SoS?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/vatlab/SoS.svg?branch=master)](https://travis-ci.org/vatlab/SoS)
[![Build Status](https://ci.appveyor.com/api/projects/status/x092eusa0tta3msw?svg=true
)](https://ci.appveyor.com/project/BoPeng/sos)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/67b766a827fb491fa473032b4f70ebb7)](https://www.codacy.com/app/BoPeng/SoS?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=vatlab/SOS&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/vatlab/SOS/badge.svg)](https://coveralls.io/github/vatlab/SOS)

**Script of Scripts (SoS)** is a Jupyter-based **polyglot notebook** that allows the use of multiple Jupyter kernels in one notebook, and
a **workflow engine** for the execution of workflows in both process- and outcome-oriented styles. It is designed for data scientists and bioinformatics who routinely work with scripts in different languages such as R, Python, Perl, and bash.

Please refer to the [SoS homepage](http://vatlab.github.io/SOS) for more information. 

We welcome and value community contributions: please [post issues](https://github.com/vatlab/SoS/issues) 
to provide us feedback or get our support; please [send pull requests](https://github.com/vatlab/SoS/pulls) 
if you have helped fixing bugs or making improvements to the source code.  

# Installing SoS 

With Python 3.6 and above installed ([anaconda python](https://www.continuum.io/downloads) is recommended), you can install the SoS Workflow System and its extensions with command

```
% pip install sos
```

You can install the latest git version of SoS with commands

```
% git clone https://github.com/vatlab/SoS.git
% cd SoS
% pip install . -U
```
although the development version can be less stable than the released
version.

You can install SoS Notebook and all language modules, and register the sos kernel with Jupyter using the following commands

```
% pip install sos-notebook
% python -m sos_notebook.install
```

although different modules might be needed for SoS Notebook to work with
other kernels. Please refer to [the installation page of sos website](https://vatlab.github.io/sos-docs/#runningsos) for details.

### Change Log of SoS and SoS Notebook

SoS Notebook 0.18.1
* [sos-notebook#178](https://github.com/vatlab/sos-notebook/issues/178): Allow adding `&` to the end of magics `%run`, `%sosrun`, and `%runfile` to execute workflow in background.
* [sos-notebook#179](https://github.com/vatlab/sos-notebook/issues/179): Remove magic `%rerun` and add magic `%runfile`
* [sos-notebook#180](https://github.com/vatlab/sos-notebook/issues/180): Add option `-r` to `%save` to execute the cell after saving it.

SoS 0.18.0
* [sos#1115](https://github.com/vatlab/SoS/issues/1115): Function `output_from` and `named_output` to support named inputs and outputs, among other new features summarized in this ticket.
* [sos#1120](https://github.com/vatlab/SoS/issues/1120): Allow paremeters `group_by`, `paired_with` etc for functions `output_from` and `named_output`.
* [sos#1125](https://github.com/vatlab/SoS/issues/1125): Set `concurrent=True` as default for substep execution.
* [sos#1132](https://github.com/vatlab/SoS/issues/1132): Deprecate action `stop_if` and replace it with `done_if` and `skip_if`
* [sos#1175](https://github.com/vatlab/SoS/issues/1175): Enforce the use of `sos_variable` to import shared variable in a step

SoS Notebook 0.18.0
* [sos-notebook#150](https://github.com/vatlab/sos-notebook/issues/150): A new side panel that works in the same fashion as JupyterLab's console window.
* [sos-notebook#154](https://github.com/vatlab/sos-notebook/issues/154): New task status table that allows reporting status, killing, and purging multiple tasks with the same tags.

SoS 0.9.16.10
* [sos#786](https://github.com/vatlab/SoS/issues/786): Support singularity. See [SoS Singularity Guide](https://vatlab.github.io/sos-docs/doc/tutorials/Singularity.html) for details.

SoS 0.9.16.0, SoS Notebook 0.9.16.0
* [sos#991](https://github.com/vatlab/SoS/issues/991): Use a new task file format to replace multiple files for each task. **This change is not backward compatible so please upgrade only after you completed and removed all existing tasks.**

SoS 0.9.15.1
* [sos-notebook#89](https://github.com/vatlab/sos-notebook/issues/89): Added templates to highlight source code using codemirror, with optional auto-generated table of contents.

SoS 0.9.14.10
* [sos#983](https://github.com/vatlab/SoS/issues/983): Allow depending on an entire process-oriented workflow using a `sos_step()` target that matches multiple steps.

SoS 0.9.14.3:
* [sos#975](https://github.com/vatlab/SoS/issues/975): Add option `-p` to generate a summary report after the completion of workflow.
* [sos#976](https://github.com/vatlab/SoS/issues/976): Much improved workflow help message (`sos run script -h`).

SoS Notebook 0.9.14.4:
* [sos-notebook#79](https://github.com/vatlab/sos-notebook/issues/79): Allow auto-completion and inspection in subkernel cells.

SoS Notebook 0.9.14.1
* [sos-notebook#74](https://github.com/vatlab/sos-notebook/issues/74): Add a `%revisions` magic to display revision history of the current document.

SoS 0.9.14.1
* [sos#925](https://github.com/vatlab/SoS/issues/924): Output summary of executed and ignored step, substeps, and tasks after the execution of workflows.

SoS Notebook 0.9.13.4
* [jupyterlab-sos#11](https://github.com/vatlab/jupyterlab-sos/issues/11): Magic `%cd` now changes directory of all subkernels

SoS Notebook 0.9.12.12
* [sos-notebook#52](https://github.com/vatlab/sos-notebook/issues/52): All new syntax highlighter that highlights expanded expressions
* [sos-notebook#58](https://github.com/vatlab/sos-notebook/issues/58): Stop removing leading comments from cells.

SoS 0.9.12.11
* [sos#922](https://github.com/vatlab/SoS/issues/922): Use user-id for docker execution (-u)
* [sos#926](https://github.com/vatlab/SoS/issues/926): Add function `zap()` to SoS path classes `path`, `paths`, `file_target`, and `sos_targets`


SoS Notebook 0.9.12.11
* [sos-notebook#44](https://github.com/vatlab/sos-notebook/issues/44): Allow sending text in markdown cells to side panel for execution.
* [sos-notebook#47](https://github.com/vatlab/sos-notebook/issues/47): Allow clear any HTML element with magic `%clear --class`
* [sos-notebook#50](https://github.com/vatlab/sos-notebook/issues/50): Re-design logo for SoS Notebook.

SoS 0.9.12.9
* [sos#914](https://github.com/vatlab/SoS/issues/914): Allow option `active` of actions and tasks to accept conditions.
* [sos#915](https://github.com/vatlab/SoS/issues/915): Automatically expand user (`~`) for SoS path types `path`, `paths` and `file_targets`.
* [sos#916](https://github.com/vatlab/SoS/issues/916): Use hashlib instead of faster xxhash under windows

SoS Notebook 0.9.12.9
* [sos-notebook#41](https://github.com/vatlab/sos-notebook/issues/41): Stop saving unused kernels in sos notebook.

SoS 0.9.12.3
* [sos#859](https://github.com/vatlab/SoS/issues/859): Introduce automatic auxiliary steps to simplify the use of makefile steps.

SoS 0.9.11.3
* [sos#879](https://github.com/vatlab/SoS/issues/879): Add action options `stdout` and `stderr` to redict output from script-executing actions. 
* [sos-notebook#42](https://github.com/vatlab/sos-notebook/issues/42): Add option `--append` to magic `%capture` .

SoS 0.9.11.2
* [sos-notebook#39](https://github.com/vatlab/sos-notebook/issues/39): Separation installation and deployment and use command `python -m sos_notebook.install` to install `sos` kernel to Jupyter.

SoS 0.9.10.19

* [sos#874](https://github.com/vatlab/SoS/issues/874): Add input option `concurrent=True` to allow parallel execution of input groups.
* [sos#874](https://github.com/vatlab/SoS/issues/874): Optimize task submission of task engines to reduce status checking 

SoS Notebook 0.9.10.17

* [sos-notebook#32](https://github.com/vatlab/sos-notebook/issues/32): Add magic `%capture` to capture output of cells 
