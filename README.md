# PSA Data Science Template

This is a standard template for data science projects. Its purpose is to meet
the needs of the data scientists and also the DDCE requirements. This example
should be considered as a starting point and changed based upon the evolution
of the project.

This template is compatible only with **Python3** and **Spark2**.


## Table of contents

<!--ts-->
   * [Template Tree](#template-tree)
   * [Getting started](#getting-started)
      * [Application and package names](#application-and-package-names)
      * [Create a new project repository on GitHub](#create-a-new-project-repository-on-gitHub)
      * [Clone the project repository and rename the package](#clone-the-project-repository-and-rename-the-package)
   * [Environment setup](#environment-setup)
      * [Python3 virtual environment](#python3-virtual-environment)
      * [User specific settings](#user-specific-settings)
   * [Working on the project](#working-on-the-project)
      * [Launch a Jupyter notebook](#launch-a-jupyter-notebook)
      * [Store the data](#store-the-data)
      * [Application logs](#application-logs)
      * [Application configuration files](#application-configuration-files)
      * [Unit tests](#unit-tests)
      * [Reset the environment](#reset-the-environment)
      * [Format code](#format-code)
      * [Git commit message guidelines](#git-commit-message-guidelines)
<!--te-->


## Template Tree

```
├── bin                               <- Directory with the python package of the project
│   ├── app_template                  <- Python package - scripts of the project
│   │   ├── configuration             <- Setup configs
│   │   │   ├── __init__.py           <- Python package initialisation
│   │   │   ├── app.py                <- Setup app config according to conf files
│   │   │   ├── data.py               <- Handle application configuration files
│   │   │   ├── mails.py              <- Setup mails config according to conf files
│   │   │   └── spark_config.py       <- Setup Spark config
│   │   ├── domain                    <- Business needs modelisation
│   │   │   ├── __init__.py           <- Python package initialisation
│   │   │   └── kpis.py               <- Example script called in app_pipeline.py
│   │   ├── infra                     <- Connexion to the data
│   │   │   ├── __init__.py           <- Python package initialisation
│   │   │   ├── db_connection.py      <- Function to create the database uri
│   │   │   ├── oracle.py             <- Read and write data in Oracle
│   │   │   └── query_builder.py      <- Functions to create sql queries
│   │   ├── pipeline                  <- Pipelines of the project
│   │   │   ├── __init__.py           <- Python package initialisation
│   │   │   ├── app_pipeline.py       <- Example pipeline with a spark session
│   │   │   └── mail_pipeline.py      <- Pipeline to send an email
│   │   └── __init__.py               <- Python package initialisation
│   │
│   ├── notebook                      <- Notebooks for analysis and testing
│   │   └── notebook_demo.ipynb       <- Example python notebook
│   │
│   └── test                          <- Unit tests
│       ├── infra                     <- Tests on the infra package
│       └── __init__.py               <- Python package initialisation
│
├── conf                              <- Environment configuration
│   ├── data                          <- Application configuration files
│   │   └── perimeter.json            <- Example configuration file
│   ├── environment                   <- User specific settings
│   │   ├── development.yml.sample    <- Development environment sample where you have to fill in the user settings
│   │   └── preproduction.yml.sample  <- Preproduction environment sample where you have to fill in the user settings
│   ├── app-profile.sh                <- Setup Python and Spark environments
│   ├── logging.conf.yml              <- Setup how to monitor the logs of the application
│   └── requirements.txt              <- Python libraries used in the project
│
├── script                            <- Scripts to build fonctionalities of the environment
│   ├── change_name.sh                <- Rename package
│   ├── describe.sh                   <- Describe main attributes of the project
│   └── notebook.sh                   <- Launch python notebook
│   
├── .gitignore                        <- Files that should be ignored by git
├── Makefile                          <- Executable to run commands of the project
└── README.md                         <- The top-level README of the repository
```


## Getting started

This git repository is a template for data science projects. To create a project
based on this template, please execute the following steps.

### Application and package names

- *Application name*: repository name on GitHub. Every PSA application follows the
same naming convention. It is composed of a trigram (`prd`) followed by two digits
(usually `00` if it's the first version of the application). The `prd` should meet
several requirements:
  - composed of exactly 3 alpha characters
  - should not already exist: see this [link](http://reflex.inetpsa.com/PV4/Applications/Prd?area=Applications)
  to verify that the prd is available
  - should be meaningful (e.g. *rep* for *reporting* is too general)

- *Package name*: python package which will contain all the scripts.
The requirements are as follows:
  - short
  - lowercase letters
  - no space, use `_` instead (and not `-`)
  - no accent
  - can't start with a digit

- Example:
  - *Application name*: `nvi00`
  - *Package name*: `new_vhl_impact`

### Create a new project repository on GitHub

Create an empty repository on GitHub with the following parameters:
- *Repository template*: brc14/app00
- *Owner*: choose the BRC related to the project (or your personal user)
- *Repository name*: the repository name you choosed (with format `prd00`)
- *Description*: add a description of the project
- Choose *Public*

### Clone the project repository and rename the package

The repository created above exists as a *remote* repository. To be able to work
on it from ARC, you have to create a *local* copy by cloning it.

Click on `Clone or download`, and copy the SSH url from the `Clone with SSH` window.
In your terminal, please run the following command.
```
$ git clone <SSH url>
```
NB: if you have problems cloning the project using SSH protocol, then your SSH key
may not be properly configured. Please see the documentation at this [link](https://shiftup.sharepoint.com/sites/datascience/_layouts/15/WopiFrame.aspx?sourcedoc={76cd4b2b-c68d-4dc7-b029-910c4869324c}&action=edit&wd=target%28Technical%20Kit.one%7C4292f160-b6f8-4df4-8fa4-b076bffa6d8c%2F5.2.1.%20SSH%20connection%7C3396b4a5-79b8-4713-a702-1360b437e23d%2F%29)
to configure it.

Second, use the command `rename` defined in the Makefile. At the invitation,
please enter the initial name of the package (by default `app_template`)
and then the new name (`package_name`).
```
$ cd prd00
$ make rename
What is the initial name?
$ app_template
What is the new name?
$ package_name
```

Third, commit your changes.
```
$ git add .
$ git commit -m "MNT: rename python package"
$ git push
```


## Environment setup

### Python3 virtual environment

This command will install a Python3 environment with all the python libraries
set by default in *requirements.txt*.
```
$ make install
```
Feel free to add any library in the *requirements.txt* file that you may need
in the project, and then run again the command above.

To activate the environment:
```
$ source conf/app-profile.sh
```

### User specific settings

The subfolder `conf` contains the environment configuration.
```
├── conf                              <- Environment configuration
   ├── environment                    <- User specific settings
   │   ├── development.yml.sample     <- Development environment sample where you have to fill in the user settings
   │   └── preproduction.yml.sample   <- Preproduction environment sample where you have to fill in the user settings
   ├── app-profile.sh                 <- Setup Python and Spark environments
   ├── logging.conf.yml               <- Setup how to monitor the logs of the application
   └── requirements.txt               <- Python libraries used in the project
```
To configure the user settings, create a `development.yml` file or
`preproduction.yml` file, depending on which environment you are working on
(development or preproduction). Use the `chmod 600` command to withdraw access
(read & write) to everyone except you.
```
$ cd conf
$ cp environment/development.yml.sample environment/development.yml
$ chmod 600 environment/development.yml
```
Then, edit `development.yml` and fill in your user settings.
Last, create the symbolic link `application.yml` of the `development.yml` file.
```
$ pwd
/gpfs/user/uXXXXXX/prd00/conf
$ ln -s environment/development.yml ./application.yml
```
It means that the `application.yml` file is like an alias of the
`development.yml` file. If you change anything in the `development.yml` file,
the modification will also be recorded in the `application.yml` file.

The new tree structure of the `conf` directory should be now:
```
├── conf                           
   ├── environment                 
   │   ├── development.yml
   │   ├── development.yml.sample  
   │   └── preproduction.yml.sample   
   ├── app-profile.sh   
   ├── application.yml          
   ├── logging.conf.yml            
   └── requirements.txt            
```
If you are in the *preproduction* mode, you just have to replace `development` by
`preproduction` in the previous commands.


## Working on the project

### Launch a Jupyter notebook

To install Jupyter and launch a notebook, run the following command.
A password is required to lock the notebook, you can set the password you want.
```
$ pwd
/gpfs/user/uXXXXXX/prd00
$ make notebook
```
NB: If it's the first time that you launch a Jupyter notebook on ARC, please
run the two following commands before:
```
$ cp -fR /gpfs/user/common/jupyter $HOME
$ cp -fR $HOME/jupyter/template/.jupyter $HOME
```

By default, notebooks are not tracked by git. If you want to add a specific one
still (for example a demo notebook), force git to add it:
```
$ git add -f bin/notebook/notebook_to_add.ipynb
```

### Store the data

The `data/` directory should be located in the root directory of the application
(`/gpfs/user/uXXXXXX/prd00`). A structure for the `data` directory could be:
```
├── data                           
   ├── raw               <- Raw data from the databases
   ├── preprocessed      <- Raw data preprocessed
   └── processed         <- Final dataframes of the project
```

### Application logs

To save the logs of the application, you have to import the class AppConfig
in your main script:
```
$ from app_template.configuration.app import AppConfig
```
Then, instantiate AppConfig at the beginning of your main function:
```
$ AppConfig()
```
To see the logs during the execution of your program:
```
$ pwd
/gpfs/user/uXXXXXX/prd00
$ tail -f log/app_template.log
```

### Application configuration files

The configuration files should be located in the `conf/data` directory.
The recommended formats are `json` or `yaml`.

To retrieve the information from the configuration files in the python package,
please configure the `DataConfig` class located in `bin/app_template/configuration/data.py`.
The information will be available as class attributes.

To use the class in your main script (example given according to the `perimeter.json` file
located in the `conf/data` directory):
```
$ from app_template.configuration.data import DataConfig
$ data_config = DataConfig()

$ data_config.vhls_perimeter
{'sites': ['PY', 'MU'], 'start_date': '15/01/20', 'end_date': '17/01/20', 'genr_door': 'EMON'}
```

### Unit tests

The unit tests of the application should be located in the `bin/test`
directory. To launch the tests:
```
$ pwd
/gpfs/user/uXXXXXX/prd00
$ make test
```
of equivalenty,
```
$ pytest bin/test bin/app_template
```
which allows [a finer grained control](http://doc.pytest.org/en/latest/usage.html#specifying-tests-selecting-tests) on which tests to run.

The [`pytest-spark`](https://pypi.org/project/pytest-spark/) package, included in the dependencies, exposes a global `spark_session` fixture that creates a PySpark session for unit tests.

### Reset the environment

If you want to reset the environment, use the command:
```
$ pwd
/gpfs/user/uXXXXXX/prd00
$ make clean
```
It will delete all the *.pyc* files and the *cache* folders.

### Format code

To format the code using PEP8 and PEP256 rules:
```
$ pwd
/gpfs/user/uXXXXXX/prd00
$ make format
```
Warning: this command will reformat every python files in the package!
If you want to keep your own formatting on some code block just add `# fmt: off`
and `# fmt: on` and your code between.
```python
# fmt: off
foo = "mycode i dont want to reformat"
# fmt: on
```

### Git commit message guidelines

The purpose of a git commit message is to summarize what you changed and why.
Committing regularly allows the other users to easily follow the development of
the code and to rework on it. To allow a good understanding of the commit
messages, the following structure has been adopted:
```
<type of change> <scope>: <description of the change>
```
where `<type of change>` can be taken from the table below (adapted from
numpy developer guide). The last column corresponds to the legacy acronym
system,

| <type of change> | Description                                        | Legacy acronyms |
|------------------|----------------------------------------------------|-----------------|
| API              | an (incompatible) API change                       |                 |
| BENCH            | changes to the benchmark suite                     |                 |
| BLD              | change related to building/CI                      |                 |
| FIX              | bug fix                                            | fix             |
| DEP              | deprecate something, or remove a deprecated object |                 |
| DOC              | documentation                                      | doc             |
| ENH              | enhancement                                        | feat           |
| MAINT (or MNT)   | maintenance commit (refactoring, typos, etc.)      | refactor, minor |
| REV              | revert an earlier commit                           |                 |
| STY              | style fix (whitespace, PEP8)                       | style           |
| TST              | addition or modification of tests                  | test            |
| REL              | related to releasing                               |                 |
| NBK              | addition or modification of notebooks              | notebook        |


- `<scope>`: name of the sub-package of the python package containing the file(s) concerned by the commit.

- `<description of the change>`: brief description of the change. Use the
present tense (e.g. "Change", not "Changed" / "Changes"), don't add any dot (.)
at the end of the description.

Example:
```
FIX configuration: correct bug dynamic allocation in build_spark_session
```
