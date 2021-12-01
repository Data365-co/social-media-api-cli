# Social Media API CLI tool

## Requirements

 - python >= 3.8
 - pip

## Installation

1. Create and activate a Python virtual environment 
   ([instructions](https://realpython.com/python-virtual-environments-a-primer/#using-virtual-environments)).
   Skip if you want to use system interpreter
1. Install required Python packages
```shell
pip3 install -r requirements.txt
```

## Usage

1. Activate the Python virtual environment. Skip if you want to use system interpreter
1. Set values for `ACCESS_TOKEN` variable in the `config.py` file
1. Read the list of commands
```shell
python3 run.py --help
```
1. Read the help for selected command 
```shell
python3 run.py facebook-post --help
```
1. Run the script and wait until finished. You can stop the script at any moment, next time it will start from where it left
```shell
python3 run.py facebook-post posts.txt --fetch-comments --max-comments 15
```
